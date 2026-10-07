import { RadarModel, HOUR, COLORS, sourceDomain } from './radar-model.mjs';

const $ = selector => document.querySelector(selector);
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => `&#${c.charCodeAt(0)};`);
const fmt = value => Number(value || 0).toLocaleString();
const itemUrl = id => `https://news.ycombinator.com/item?id=${Number(id)}`;
const age = ms => ms < 60_000 ? `${Math.max(0, Math.floor(ms / 1000))}s` : ms < HOUR ? `${Math.floor(ms / 60_000)}m` : `${Math.floor(ms / HOUR)}h`;
const clock = ts => new Date(ts).toLocaleTimeString([], { hour12: false });
const plain = html => new DOMParser().parseFromString(String(html || '').replace(/<p>/gi, ' '), 'text/html').body.textContent || '';
function readPalette() {
  const styles = getComputedStyle(document.documentElement);
  const token = name => styles.getPropertyValue(`--${name}`).trim();
  return {
    bg: token('bg'), muted: token('muted'), accent: token('accent'), gain: token('gain'),
    rankUp: token('rank-up'), rankDown: token('rank-down'), rowUp: token('row-up'), rowDown: token('row-down'),
    shadow: token('shadow-rgb'), chartGrid: token('chart-grid'), radarGrid: token('radar-grid'),
    beam: token('radar-beam'), cool: token('radar-cool').split(',').map(Number), warm: token('radar-warm').split(',').map(Number),
    contact: token('radar-contact'), pulse: token('radar-pulse'),
    series: Object.fromEntries(Object.keys(COLORS).map(kind => [kind, token(kind)])),
  };
}
let palette = readPalette();
const model = new RadarModel();
let snapshot = model.snapshot(), ranking = 'front', filter = 'all', allStories = false, submissionLimit = 8;
let connected = false, streamSource = '', hits = [], selectedHit = null, chartIndex = null;
const rendered = new Map(), snippets = new Map();
const reducedMotion = matchMedia('(prefers-reduced-motion: reduce)');
const activeEffects = new Set(), effectsByElement = new WeakMap();
const motionAllowed = () => !reducedMotion.matches && !document.hidden;
const inView = element => {
  const rect = element.getBoundingClientRect();
  return rect.bottom > 0 && rect.top < innerHeight && rect.width > 0;
};
const easing = 'cubic-bezier(.22, 1, .36, 1)';
function animate(element, channel, frames, duration = 600, delay = 0, options = {}) {
  if (!motionAllowed()) return;
  let effects = effectsByElement.get(element);
  if (!effects) effectsByElement.set(element, effects = new Map());
  effects.get(channel)?.cancel();
  const effect = element.animate(frames, { duration, delay, easing, ...options });
  effects.set(channel, effect); activeEffects.add(effect);
  const clean = () => {
    activeEffects.delete(effect);
    if (effects.get(channel) === effect) effects.delete(channel);
  };
  effect.onfinish = clean; effect.oncancel = clean;
  return effect;
}
const numberText = (value, signed = false) => `${signed && value > 0 ? '+' : ''}${fmt(value)}`;
const number = (value, { signed = false, rank = false } = {}) => {
  const numeric = Number(value || 0), text = numberText(numeric, signed);
  return `<span class="number-slot" data-number="${numeric}" data-signed="${signed}" data-rank="${rank}"><span class="number-current">${esc(text)}</span></span>`;
};
function updateNumber(element, value) {
  const previous = Number(element.dataset.number);
  if (previous === value) return;
  const current = element.querySelector('.number-current');
  const signed = element.dataset.signed === 'true', isRank = element.dataset.rank === 'true';
  const text = numberText(value, signed), oldText = current.textContent;
  // Accessible text and the data model always expose the latest exact value.
  element.dataset.number = String(value);
  effectsByElement.get(current)?.get('digits')?.cancel();
  element.querySelectorAll('.number-previous').forEach(old => {
    old.getAnimations().forEach(effect => effect.cancel()); old.remove();
  });
  current.textContent = text;
  if (!motionAllowed() || !inView(element)) return;
  const old = document.createElement('span');
  old.className = 'number-previous'; old.setAttribute('aria-hidden', 'true'); old.textContent = oldText;
  element.append(old);
  const increasing = value > previous, direction = (isRank ? !increasing : increasing) ? 1 : -1;
  const options = { easing: 'cubic-bezier(.4, 0, .2, 1)' };
  const incoming = animate(current, 'digits', [
    { transform: `translateY(${direction * 115}%)`, opacity: .55 },
    { transform: 'translateY(0)', opacity: 1 },
  ], 850, 0, options);
  animate(old, 'digits', [
    { transform: 'translateY(0)', opacity: 1 },
    { transform: `translateY(${-direction * 115}%)`, opacity: .25 },
  ], 850, 0, options);
  const baseColor = getComputedStyle(element).color;
  animate(element, 'number-color', [
    { color: isRank ? (increasing ? palette.rankDown : palette.rankUp) : palette.gain },
    { color: baseColor },
  ], 1500);
  // Each update owns its outgoing glyphs; a cancelled older transition cannot remove newer ones.
  incoming.finished.then(() => old.remove(), () => old.remove());
}
function setMetric(element, value) {
  if (value === null) { element.textContent = '—'; return; }
  const slot = element.querySelector('.number-slot');
  if (!slot) element.innerHTML = number(value);
  else updateNumber(slot, value);
}

// Patch inside a keyed row so live updates retain links, keyboard focus and CSS transitions.
function patchNode(current, next) {
  if (current.nodeType !== next.nodeType || current.nodeName !== next.nodeName) {
    current.replaceWith(next.cloneNode(true)); return;
  }
  if (current.nodeType === Node.TEXT_NODE) {
    if (current.textContent !== next.textContent) current.textContent = next.textContent;
    return;
  }
  if (current.nodeType !== Node.ELEMENT_NODE) return;
  if (current.hasAttribute('data-number') && next.hasAttribute('data-number')) {
    updateNumber(current, Number(next.dataset.number)); return;
  }
  for (const { name } of [...current.attributes]) if (!next.hasAttribute(name)) current.removeAttribute(name);
  for (const { name, value } of [...next.attributes]) if (current.getAttribute(name) !== value) current.setAttribute(name, value);
  const oldChildren = [...current.childNodes], newChildren = [...next.childNodes];
  newChildren.forEach((child, i) => oldChildren[i] ? patchNode(oldChildren[i], child) : current.append(child.cloneNode(true)));
  oldChildren.slice(newChildren.length).forEach(child => child.remove());
}
function html(selector, markup) {
  if (rendered.get(selector) === markup) return;
  const list = $(selector), template = document.createElement('template');
  template.innerHTML = markup;
  const focused = list.contains(document.activeElement) ? document.activeElement : null;
  const previous = new Map([...list.children].filter(row => row.dataset.key).map(row => [row.dataset.key, row]));
  const scrollingFeed = selector === '#feed' && list.scrollTop > 4;
  const anchor = scrollingFeed ? [...list.children].find(row => row.getBoundingClientRect().bottom > list.getBoundingClientRect().top) : null;
  const anchorTop = anchor?.getBoundingClientRect().top;
  const moving = motionAllowed() && inView(list) && !scrollingFeed;
  const listTop = list.getBoundingClientRect().top;
  const positions = moving ? new Map([...previous].map(([key, row]) => [key, {
    visual: row.getBoundingClientRect().top - listTop,
    layout: row.offsetTop,
  }])) : new Map();
  const nextRows = [...template.content.children], changes = [];
  nextRows.forEach((next, i) => {
    const row = previous.get(next.dataset.key) || next;
    if (row !== next) patchNode(row, next);
    if (list.children[i] !== row) list.insertBefore(row, list.children[i] || null);
    changes.push({ row, inserted: row === next });
  });
  const retained = new Set(changes.map(change => change.row));
  [...list.children].filter(row => !retained.has(row)).forEach(row => {
    row.getAnimations({ subtree: true }).forEach(effect => effect.cancel()); row.remove();
  });
  if (focused?.isConnected && document.activeElement !== focused) focused.focus({ preventScroll: true });
  if (anchor?.isConnected) list.scrollTop += anchor.getBoundingClientRect().top - anchorTop;
  if (moving) changes.forEach(({ row, inserted }, i) => {
    if (!inView(row)) return;
    if (inserted) {
      animate(row, 'position', [{ opacity: 0, transform: 'translateY(18px)' }, { opacity: 1, transform: 'translateY(0)' }], 650, Math.min(i, 4) * 35);
      return;
    }
    const before = positions.get(row.dataset.key), target = row.offsetTop;
    // Layout coordinates ignore the current transform. A value-only update leaves an ongoing
    // move alone; a second reorder starts from the position the reader is actually seeing.
    if (!before || Math.abs(before.layout - target) <= 1) return;
    const offset = before.visual - target, up = before.layout > target;
    const tint = up ? palette.rowUp : palette.rowDown;
    animate(row, 'position', [
      { offset: 0, transform: `translateY(${offset}px)`, backgroundColor: palette.bg, zIndex: up ? 3 : 2, boxShadow: `0 0 0 rgba(${palette.shadow},0)` },
      { offset: .10, transform: `translateY(${offset}px)`, backgroundColor: tint, zIndex: up ? 3 : 2, boxShadow: `0 4px 14px rgba(${palette.shadow},.10)`, easing: 'cubic-bezier(.4,0,.2,1)' },
      { offset: .88, transform: 'translateY(0)', backgroundColor: tint, zIndex: up ? 3 : 2, boxShadow: `0 4px 14px rgba(${palette.shadow},.06)` },
      { offset: 1, transform: 'translateY(0)', backgroundColor: palette.bg, zIndex: 0, boxShadow: `0 0 0 rgba(${palette.shadow},0)` },
    ], 1150, 0, { easing: 'linear' });
  });
  rendered.set(selector, markup);
}
const empty = message => `<li class="empty">${esc(message)}</li>`;
const article = item => `<a class="story-link" href="${itemUrl(item.id)}" target="_blank" rel="noopener">${esc(item.title || 'Untitled story')}</a><span class="story-domain">${esc(sourceDomain(item.url))}<span class="mobile-comments"> · ${fmt(item.descendants)} comments</span><span class="mobile-author"> · ${esc(item.by || 'anonymous')}</span></span>`;
const historyTitle = row => `Change over ${Math.floor(row.gain.duration / 60_000)} min of available observations`;
const triangle = down => `<svg viewBox="0 0 10 10" aria-hidden="true"><path fill="currentColor" d="${down ? 'M0 1h10L5 9z' : 'M5 1l5 8H0z'}"/></svg>`;
function movement(value) {
  if (value === null) return '<span class="movement new" aria-label="New to this ranking">new</span>';
  if (!value) return '<span class="movement" aria-label="Rank unchanged">—</span>';
  return `<span class="movement ${value > 0 ? 'up' : 'down'}" aria-label="${value > 0 ? 'Up' : 'Down'} ${Math.abs(value)} ranks">${triangle(value < 0)}${Math.abs(value)}</span>`;
}

function renderNews() {
  const rows = ranking === 'front' ? snapshot.front : snapshot.rising;
  const shown = allStories ? rows : rows.slice(0, 8);
  html('#front', shown.map((row, index) => `<li class="story-row" data-key="${row.item.id}" data-revision="${row.item.score}:${row.item.descendants}:${row.gain.points}">
    <div class="rank"><span>${number(ranking === 'front' ? row.rank : index + 1, { rank: true })}</span>${ranking === 'front' ? movement(row.movement) : ''}</div>
    <div class="story-content">${article(row.item)}</div>
    <div class="score" aria-label="${fmt(row.item.score)} points">${number(row.item.score)}${row.gain.points ? `<small class="${row.gain.points > 0 ? 'gain' : 'down'}" title="${historyTitle(row)}">${number(row.gain.points, { signed: true })}</small>` : ''}</div>
    <a class="comment-count" href="${itemUrl(row.item.id)}" target="_blank" rel="noopener" aria-label="${fmt(row.item.descendants)} comments">${number(row.item.descendants)}</a>
  </li>`).join('') || empty(ranking === 'front' ? 'Waiting for the next front-page snapshot…' : 'Collecting score history. Rising stories will appear as points change.'));
  const button = $('#show-stories');
  button.hidden = rows.length <= 8;
  button.textContent = allStories ? 'Show fewer stories ↑' : `Show all ${rows.length} stories ↓`;
  $('#ranking-note').textContent = ranking === 'front' ? 'Rank change · up to 30 min' : 'Points gained · up to 30 min';
}

function renderFeed() {
  html('#feed', snapshot.feed.slice(0, 50).map(({ ts, item }) => {
    const comment = item.type === 'comment';
    if (!snippets.has(item.id)) snippets.set(item.id, plain(item.text).slice(0, 240));
    const excerpt = comment ? snippets.get(item.id) : item.title || 'Untitled submission';
    return `<li class="feed-item" data-key="${item.id}"><div class="feed-meta"><time datetime="${new Date(ts).toISOString()}">${clock(ts)}</time><span>${comment ? 'Comment' : esc((item.type || 'story').replace(/^./, c => c.toUpperCase()))}</span></div><a class="feed-link" href="${itemUrl(item.id)}" target="_blank" rel="noopener" title="${esc(excerpt)}"><strong>${esc(item.by || 'anonymous')}</strong> ${esc(excerpt)}</a></li>`;
  }).join('') || empty('Waiting for new stories and comments…'));
  const ids = new Set(snapshot.feed.map(e => e.item.id));
  for (const id of snippets.keys()) if (!ids.has(id)) snippets.delete(id);
}

function renderInsights() {
  html('#conversations', snapshot.conversations.slice(0, 5).map((row, i) => `<li class="conversation-row" data-key="${row.item.id}" data-revision="${row.gain.comments}"><span class="dim">${number(i + 1, { rank: true })}</span><div class="story-content">${article(row.item)}</div><div class="score" title="${historyTitle(row)}"><span class="gain">${number(row.gain.comments, { signed: true })}</span><small>comments</small></div></li>`).join('') || empty('Collecting comment history. Growth appears after a story is observed more than once.'));
  const domains = snapshot.domains.slice(0, 6), count = snapshot.front.length;
  $('#source-context').textContent = count ? `Share of the current top ${count}` : 'Share of the current front page';
  html('#sources', domains.map(([name, n]) => `<li class="source-row" data-key="${esc(name)}" data-revision="${n}"><span class="source-name" title="${esc(name)}">${esc(name)}</span><span class="source-track" aria-hidden="true"><i class="source-fill" style="width:${n / count * 100}%"></i></span><span class="source-count" aria-label="${n} stories">${number(n)}</span></li>`).join('') || empty('Waiting for the front page…'));
  const remainder = count - domains.reduce((total, [, n]) => total + n, 0);
  $('#source-remainder').textContent = count ? remainder ? `All remaining sources · ${remainder} ${remainder === 1 ? 'story' : 'stories'}` : 'All front-page sources shown' : '';
  for (const [selector, labels, message] of [
    ['#subjects', snapshot.subjects, 'Waiting for subject analysis…'],
    ['#emerging', snapshot.emergingTopics, 'Waiting for comment analysis…'],
  ]) {
    html(selector, labels.map(([label, count]) => `<li class="analysis-row" data-key="${esc(label)}" data-revision="${count}"><span>${esc(label)}</span><span class="analysis-count">${number(count)} ${count === 1 ? 'story' : 'stories'}</span></li>`).join('') || empty(message));
  }
}

function renderLatest() {
  const rows = snapshot.submissions.filter(({ item }) => filter === 'all' || new RegExp(`^${filter} HN:`, 'i').test(item.title || ''));
  html('#latest', rows.slice(0, submissionLimit).map(({ ts, item }) => `<li class="latest-row" data-key="${item.id}" data-revision="${item.score}:${item.descendants}"><time datetime="${new Date(ts).toISOString()}" title="${clock(ts)}">${age(snapshot.now - ts)} ago</time><div class="story-content">${article(item)}</div><span class="author">${esc(item.by || 'anonymous')}</span><span class="score" aria-label="${fmt(item.score)} points">${number(item.score)}</span><a class="comment-count" href="${itemUrl(item.id)}" target="_blank" rel="noopener" aria-label="${fmt(item.descendants)} comments">${number(item.descendants)}</a></li>`).join('') || empty(filter === 'all' ? 'No new stories received in the last hour yet.' : `No ${filter === 'show' ? 'Show' : 'Ask'} HN stories received in the last hour.`));
  $('#show-latest').hidden = rows.length <= submissionLimit;
  $('#show-latest').textContent = `Show ${Math.min(12, rows.length - submissionLimit)} more submissions ↓`;
}

function renderStatus() {
  const last = snapshot.lastTs;
  const stale = connected && last && Date.now() - last > 120_000;
  $('#status').className = `connection ${connected ? stale ? 'stale' : 'live' : 'offline'}`;
  $('#state').textContent = connected ? stale ? 'Waiting for updates' : last ? 'Live' : 'Loading' : 'Reconnecting';
  $('#status').title = streamSource;
  $('#last-event').textContent = last ? `Last event ${age(Date.now() - last)} ago` : 'Waiting for the stream';
}

function render() {
  snapshot = model.snapshot();
  for (const [key, value] of Object.entries(snapshot.counts)) {
    const element = $(`#k-${key}`);
    if (element) setMetric(element, snapshot.lastTs ? value : null);
  }
  setMetric($('#k-rate'), snapshot.lastTs ? snapshot.rate : null);
  renderStatus(); renderNews(); renderFeed(); renderInsights(); renderLatest(); updateChartTarget(); renderChartSummary(); updateRadarTargets(); drawChart(); drawRadar(); syncMotionLoop();
}

const stream = new EventSource('/events');
stream.onopen = () => { connected = true; renderStatus(); syncMotionLoop(); };
stream.onerror = () => { connected = false; renderStatus(); syncMotionLoop(); };
stream.onmessage = event => {
  try { model.ingest(JSON.parse(event.data)); }
  catch (error) { console.warn('Skipped an invalid stream event:', error.message); }
};
stream.addEventListener('source', event => { streamSource = event.data; });

function selectRanking(button) {
  ranking = button.dataset.ranking;
  allStories = false;
  for (const tab of document.querySelectorAll('[data-ranking]')) {
    const selected = tab === button;
    tab.setAttribute('aria-selected', String(selected)); tab.tabIndex = selected ? 0 : -1;
  }
  $('#stories-panel').setAttribute('aria-labelledby', button.id);
  renderNews();
}
const rankingTabs = [...document.querySelectorAll('[data-ranking]')];
rankingTabs.forEach((button, index) => {
  button.addEventListener('click', () => selectRanking(button));
  button.addEventListener('keydown', event => {
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? rankingTabs.length - 1 : (index + (event.key === 'ArrowRight' ? 1 : -1) + rankingTabs.length) % rankingTabs.length;
    rankingTabs[next].focus(); selectRanking(rankingTabs[next]);
  });
});
$('#show-stories').addEventListener('click', () => {
  allStories = !allStories; renderNews();
  if (!allStories) $('#tab-' + ranking).scrollIntoView({ block: 'start' });
});
document.querySelectorAll('[data-filter]').forEach(button => button.addEventListener('click', () => {
  filter = button.dataset.filter; submissionLimit = 8;
  document.querySelectorAll('[data-filter]').forEach(tab => tab.setAttribute('aria-pressed', String(tab === button)));
  renderLatest();
}));
$('#show-latest').addEventListener('click', () => { submissionLimit += 12; renderLatest(); });

function canvasContext(canvas) {
  const { width: w, height: h } = canvas.getBoundingClientRect(), dpr = devicePixelRatio || 1;
  if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(h * dpr)) {
    canvas.width = Math.round(w * dpr); canvas.height = Math.round(h * dpr);
  }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, w, h);
  ctx.font = '11px -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
  ctx.lineWidth = 1; ctx.textBaseline = 'alphabetic'; ctx.textAlign = 'left';
  return { ctx, w, h };
}

const chart = $('#chart'), chartTip = $('#chart-tip');
let chartMotion = null, chartSignature = '', radarVisible = true, chartVisible = false;
function chartFrame(time = performance.now()) {
  if (!chartMotion) return { bins: snapshot.bins, scale: 3 };
  const progress = motionAllowed() ? Math.min(1, Math.max(0, (time - chartMotion.start) / 500)) : 1;
  const mix = 1 - (1 - progress) ** 3;
  return {
    bins: chartMotion.target.map((bin, i) => Object.fromEntries(Object.keys(COLORS).map(key => [key, chartMotion.from[i][key] + (bin[key] - chartMotion.from[i][key]) * mix]))),
    scale: chartMotion.oldScale + (chartMotion.scale - chartMotion.oldScale) * mix,
  };
}
function updateChartTarget() {
  const signature = JSON.stringify([snapshot.binEnd, snapshot.bins]);
  if (signature === chartSignature) return;
  const current = chartFrame(), target = snapshot.bins.map(bin => ({ ...bin }));
  const peak = Math.max(0, ...target.map(bin => Object.values(bin).reduce((a, b) => a + b, 0)));
  const scale = Math.max(3, Math.ceil(peak / 3) * 3);
  const shift = chartMotion ? Math.max(0, Math.round((snapshot.binEnd - chartMotion.end) / 60_000)) : 0;
  const animateChange = chartMotion && chartVisible && motionAllowed();
  chartMotion = { target, from: animateChange ? current.bins.map((_, i) => current.bins[i + shift] || Object.fromEntries(Object.keys(COLORS).map(key => [key, 0]))) : target,
    oldScale: animateChange ? current.scale : scale, scale, start: performance.now() - (animateChange ? 0 : 500), end: snapshot.binEnd };
  chartSignature = signature;
}
function drawChart(time = performance.now()) {
  const { ctx, w, h } = canvasContext(chart), left = 34, bottom = h - 24, top = 13, plotW = w - left - 3;
  if (plotW <= 0 || bottom <= top) return;
  const totals = snapshot.bins.map(bin => Object.values(bin).reduce((a, b) => a + b, 0));
  const peak = Math.max(0, ...totals), frame = chartFrame(time), scale = frame.scale;
  for (let i = 0; i <= 3; i++) {
    const y = bottom - (bottom - top) * i / 3;
    ctx.strokeStyle = palette.chartGrid; ctx.beginPath(); ctx.moveTo(left, y); ctx.lineTo(w, y); ctx.stroke();
    ctx.fillStyle = palette.muted; ctx.textAlign = 'right'; ctx.fillText(fmt(Math.round(scale * i / 3)), left - 9, y + 4);
  }
  frame.bins.forEach((bin, index) => {
    let y = bottom;
    for (const [key, color] of Object.entries(palette.series)) {
      const height = bin[key] / scale * (bottom - top);
      ctx.fillStyle = color;
      ctx.fillRect(left + index * plotW / 60 + 1, y - height, Math.max(1, plotW / 60 - 3), height);
      y -= height;
    }
  });
  ctx.fillStyle = palette.muted; ctx.textAlign = 'left'; ctx.fillText('-60m', left, h - 3);
  ctx.textAlign = 'center'; ctx.fillText('-30m', left + plotW / 2, h - 3);
  ctx.textAlign = 'right'; ctx.fillText('Now', w, h - 3);
  if (chartIndex !== null) updateChartTip(chartIndex);
}
function renderChartSummary() {
  const peak = Math.max(0, ...snapshot.bins.map(bin => Object.values(bin).reduce((a, b) => a + b, 0)));
  $('#other-legend').hidden = !snapshot.counts.other;
  const total = Object.values(snapshot.counts).reduce((a, b) => a + b, 0);
  const summary = snapshot.lastTs ? `${fmt(total)} events in the last hour · Peak ${fmt(peak)} / min · Current minute is partial` : 'Waiting for activity…';
  $('#chart-summary').textContent = summary;
  chart.setAttribute('aria-label', `Events per minute. ${summary}. ${Object.entries(snapshot.counts).map(([kind, value]) => `${fmt(value)} ${kind} events`).join(', ')}.`);
  if (chartIndex !== null) updateChartTip(chartIndex);
}
function showTip(tip, parent, x, y) {
  tip.hidden = false;
  tip.style.left = `${Math.max(0, Math.min(x + 12, parent.clientWidth - tip.offsetWidth))}px`;
  tip.style.top = `${Math.max(0, Math.min(y + 12, parent.clientHeight - tip.offsetHeight))}px`;
}
function updateChartTip(index) {
  const bin = snapshot.bins[index], ts = snapshot.binEnd - (60 - index) * 60_000;
  chartTip.innerHTML = `<strong>${new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}${index === 59 ? ' · partial minute' : ''}</strong><br>${Object.entries(bin).filter(([, n]) => n).map(([key, n]) => `${fmt(n)} ${key === 'story' ? 'stories' : key === 'other' ? 'other events' : key + 's'}`).join(' · ') || 'No events observed'}`;
}
chart.addEventListener('pointermove', event => {
  const rect = chart.getBoundingClientRect(), x = event.clientX - rect.left, y = event.clientY - rect.top;
  chartIndex = Math.max(0, Math.min(59, Math.floor((x - 34) / (rect.width - 37) * 60)));
  updateChartTip(chartIndex); showTip(chartTip, chart.parentElement, x, y);
});
chart.addEventListener('pointerleave', () => { chartIndex = null; chartTip.hidden = true; });

const radar = $('#radar'), radarTip = $('#tip'), TAU = Math.PI * 2;
const bearing = id => (Math.imul(id, 2654435761) >>> 0) / 2 ** 32 * TAU;
const ringRadius = rank => .1 + .78 * (rank - 1) / 29;
const blips = new Map();
let pings = [], previousRadarFrame = 0, radarFrame = 0, sweepAngle = 0;
function updateRadarTargets() {
  const ids = new Set(snapshot.front.map(row => row.item.id));
  for (const id of blips.keys()) if (!ids.has(id)) blips.delete(id);
  for (const row of snapshot.front) {
    const size = Math.min(13, 3 + Math.sqrt(Math.max(0, row.item.score || 0)) / 3.5);
    const previous = blips.get(row.item.id);
    if (previous && row.changed > previous.changed && snapshot.now - row.changed < 15_000 && motionAllowed() && radarVisible) {
      pings.push({ id: row.item.id, start: performance.now() });
    }
    blips.set(row.item.id, { r: previous?.r ?? (motionAllowed() && radarVisible ? 1 : ringRadius(row.rank)),
      size: previous?.size ?? size, target: ringRadius(row.rank), targetSize: size, changed: row.changed });
  }
  pings = pings.filter(ping => performance.now() - ping.start < 1600).slice(-60);
}
function drawRadar(time = performance.now()) {
  const { ctx, w, h } = canvasContext(radar), radius = Math.max(0, Math.min(w, h) / 2 - 10), cx = w / 2, cy = h / 2;
  if (!radius) return;
  const at = (angle, r) => [cx + Math.cos(angle) * r * radius, cy + Math.sin(angle) * r * radius];
  const animated = motionAllowed() && radarVisible && connected;
  const step = animated ? 1 - Math.exp(-Math.min(64, Math.max(0, time - previousRadarFrame)) / 170) : 1;
  previousRadarFrame = time;
  if (animated) sweepAngle = time / 6000 % 1 * TAU;
  ctx.strokeStyle = palette.radarGrid; ctx.beginPath();
  for (const r of [ringRadius(10), ringRadius(20), ringRadius(30), 1]) { ctx.moveTo(cx + r * radius, cy); ctx.arc(cx, cy, r * radius, 0, TAU); }
  for (let a = 0; a < TAU - .01; a += TAU / 12) { ctx.moveTo(cx, cy); ctx.lineTo(...at(a, 1)); }
  ctx.stroke();
  // Keep the original six-second sweep legible in either theme.
  const beam = ctx.createConicGradient(sweepAngle, cx, cy);
  beam.addColorStop(0, `rgba(${palette.beam},0)`);
  beam.addColorStop(.78, `rgba(${palette.beam},0)`);
  beam.addColorStop(.93, `rgba(${palette.beam},.045)`);
  beam.addColorStop(1, `rgba(${palette.beam},.17)`);
  ctx.fillStyle = beam; ctx.beginPath(); ctx.arc(cx, cy, radius, 0, TAU); ctx.fill();
  ctx.strokeStyle = `rgba(${palette.beam},.48)`;
  ctx.beginPath(); ctx.moveTo(cx, cy); ctx.lineTo(...at(sweepAngle, 1)); ctx.stroke();
  ctx.fillStyle = palette.muted;
  for (const rank of [10, 20, 30]) ctx.fillText(`#${rank}`, cx + 7, cy - ringRadius(rank) * radius + 14);
  hits = [];
  for (const { item, ts } of snapshot.submissions) {
    if (snapshot.now - ts > 600_000) continue;
    const [x, y] = at(bearing(item.id), .95), size = 2.5;
    ctx.fillStyle = palette.contact; ctx.fillRect(x - size, y - size, size * 2, size * 2);
    hits.push({ x, y, size, row: { item, gain: { points: 0 }, rank: null } });
  }
  for (const row of snapshot.front) {
    const blip = blips.get(row.item.id);
    if (!blip) continue;
    blip.r += (blip.target - blip.r) * step; blip.size += (blip.targetSize - blip.size) * step;
    const angle = bearing(row.item.id);
    const [x, y] = at(angle, blip.r), heat = Math.min(1, Math.max(0, row.gain.points) / 50);
    const start = palette.cool, end = palette.warm;
    const color = start.map((v, i) => Math.round(v + (end[i] - v) * heat));
    const size = blip.size, glow = animated ? Math.exp(-((sweepAngle - angle + TAU) % TAU) / .65) : 0;
    ctx.fillStyle = `rgba(${color.join(',')},${.76 + glow * .24})`;
    ctx.beginPath(); ctx.arc(x, y, size, 0, TAU); ctx.fill();
    ctx.strokeStyle = palette.bg; ctx.stroke();
    if (row.rank === 1) { ctx.fillStyle = palette.muted; ctx.fillText('#1', x + size + 4, y + 4); }
    if (selectedHit?.row.item.id === row.item.id) {
      ctx.strokeStyle = palette.accent; ctx.beginPath(); ctx.arc(x, y, size + 4, 0, TAU); ctx.stroke();
    }
    hits.push({ x, y, size, row });
  }
  pings = pings.filter(ping => time - ping.start < 1600);
  if (animated) for (const ping of pings) {
    const hit = hits.find(hit => hit.row.item.id === ping.id);
    if (!hit) continue;
    const progress = Math.max(0, (time - ping.start) / 1600);
    ctx.strokeStyle = `rgba(${palette.pulse},${.4 * (1 - progress) ** 2})`;
    ctx.beginPath(); ctx.arc(hit.x, hit.y, hit.size + 3 + progress * 23, 0, TAU); ctx.stroke();
  }
}
function hitAt(event) {
  const box = radar.getBoundingClientRect(), x = event.clientX - box.left, y = event.clientY - box.top;
  const hit = hits.filter(hit => Math.hypot(hit.x - x, hit.y - y) < hit.size + 5).sort((a, b) => Math.hypot(a.x - x, a.y - y) - Math.hypot(b.x - x, b.y - y))[0];
  return { hit, x, y };
}
radar.addEventListener('pointermove', event => {
  const { hit, x, y } = hitAt(event); selectedHit = hit || null;
  radar.style.cursor = hit ? 'pointer' : '';
  if (!hit) { radarTip.hidden = true; drawRadar(); return; }
  const { item, rank, gain } = hit.row;
  radarTip.innerHTML = `<strong>${rank ? '#' + rank : 'New submission'} · ${fmt(item.score)} points · ${fmt(item.descendants)} comments</strong><br>${esc(item.title)}${gain.points > 0 ? `<br><span class="gain">+${fmt(gain.points)} points · up to 30 min observed</span>` : ''}`;
  showTip(radarTip, radar.parentElement, x, y); drawRadar();
});
radar.addEventListener('pointerleave', () => { selectedHit = null; radarTip.hidden = true; drawRadar(); });
radar.addEventListener('click', event => { const { hit } = hitAt(event); if (hit) window.open(itemUrl(hit.row.item.id), '_blank', 'noopener'); });
// One render loop, paused when the page or both charts are out of view.
function syncMotionLoop() {
  const needed = motionAllowed() && ((radarVisible && connected) || (chartVisible && chartMotion && performance.now() - chartMotion.start < 500));
  if (needed && !radarFrame) radarFrame = requestAnimationFrame(motionFrame);
  if (!needed && radarFrame) { cancelAnimationFrame(radarFrame); radarFrame = 0; }
}
function motionFrame(time) {
  radarFrame = 0;
  if (!motionAllowed()) return;
  if (radarVisible && connected) drawRadar(time);
  if (chartVisible && chartMotion && time - chartMotion.start < 550) drawChart(time);
  syncMotionLoop();
}
const chartObserver = new IntersectionObserver(entries => {
  for (const entry of entries) {
    if (entry.target === radar) radarVisible = entry.isIntersecting;
    if (entry.target === chart) chartVisible = entry.isIntersecting;
  }
  syncMotionLoop();
});
chartObserver.observe(radar); chartObserver.observe(chart);
function motionPreferenceChanged() {
  for (const effect of activeEffects) effect.cancel();
  pings = []; previousRadarFrame = 0;
  render(); syncMotionLoop();
}
reducedMotion.addEventListener('change', motionPreferenceChanged);
document.addEventListener('visibilitychange', motionPreferenceChanged);
const systemTheme = matchMedia('(prefers-color-scheme: dark)');
const themeSelect = $('#theme-select');
let themePreference = document.documentElement.dataset.themePreference || 'system';
function applyTheme(preference, persist = false) {
  themePreference = ['light', 'dark', 'system'].includes(preference) ? preference : 'system';
  const theme = themePreference === 'system' ? (systemTheme.matches ? 'dark' : 'light') : themePreference;
  const root = document.documentElement;
  if (root.dataset.theme !== theme) {
    // Settle finite transitions before replacing their palette; no old white rows remain in dark mode.
    for (const effect of activeEffects) effect.finish();
  }
  root.dataset.themePreference = themePreference;
  root.dataset.theme = theme;
  themeSelect.value = themePreference;
  palette = readPalette();
  document.querySelector('meta[name="theme-color"]').content = palette.bg;
  if (persist) { try { localStorage.setItem('hn-radar-theme', themePreference); } catch {} }
  drawChart(); drawRadar(); syncMotionLoop();
}
themeSelect.addEventListener('change', () => applyTheme(themeSelect.value, true));
systemTheme.addEventListener('change', () => { if (themePreference === 'system') applyTheme('system'); });
window.addEventListener('storage', event => {
  if (event.key === 'hn-radar-theme' || event.key === null) applyTheme(event.newValue || 'system');
});
applyTheme(themePreference);
new ResizeObserver(() => { drawChart(); drawRadar(); }).observe($('.app'));
setInterval(render, 1000);
render();
