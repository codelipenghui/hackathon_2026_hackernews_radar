export const HOUR = 3_600_000;
export const WINDOW = 30 * 60_000;
export const COLORS = { update: '#87bd9a', comment: '#83afe0', profile: '#b69bd6', story: '#ee925e', other: '#b9bdc5' };
export const sourceDomain = url => {
  try { return new URL(url).hostname.replace(/^www\./, '') || 'Hacker News'; }
  catch { return 'Hacker News'; }
};
const visible = item => item && !item.dead && !item.deleted;

// A rolling, client-side view of the observed stream; no estimates of unobserved HN traffic.
export class RadarModel {
  events = [];
  stories = new Map();
  submissions = new Map();
  feed = new Map();
  subjects = new Map();
  emergingTopics = new Map();
  seen = new Map();
  front = [];
  frontTs = 0;
  frontHistory = [];
  lastTs = 0;

  track(item, ts) {
    if (!item?.id) return;
    let story = this.stories.get(item.id);
    if (!story) this.stories.set(item.id, story = { item, ts, history: [], changed: 0 });
    const sample = { ts, points: item.score || 0, comments: item.descendants || 0 };
    const existing = story.history.findIndex(h => h.ts === ts);
    if (existing < 0) story.history.push(sample);
    else story.history[existing] = sample;
    story.history.sort((a, b) => a.ts - b.ts);
    if (ts >= story.ts) {
      if (story.item.score !== item.score || story.item.descendants !== item.descendants) story.changed = ts;
      story.item = item;
      story.ts = ts;
    }
  }

  ingest(event) {
    const ts = Number(event.ts), kind = String(event.kind).toUpperCase(), item = event.item;
    const knownKind = ['NEW', 'UPDATE', 'PROFILE', 'TOP'].includes(kind);
    const storyId = Number(event.story_id);
    const hasAnalysis = Number.isSafeInteger(storyId) && storyId > 0
      && (Array.isArray(event.subjects) || Array.isArray(event.emerging_topics));
    if (!Number.isFinite(ts) || (!knownKind && !hasAnalysis)) return;
    const key = JSON.stringify(event);
    if (this.seen.has(key)) return;
    this.seen.set(key, ts);
    this.lastTs = Math.max(this.lastTs, ts);
    if (hasAnalysis) {
      for (const [field, store] of [['subjects', this.subjects], ['emerging_topics', this.emergingTopics]]) {
        if (!Array.isArray(event[field]) || (store.get(storyId)?.ts ?? -Infinity) > ts) continue;
        const labels = [...new Set(event[field].filter(label => typeof label === 'string').map(label => label.trim()).filter(Boolean))];
        store.set(storyId, { ts, labels });
      }
    }
    if (!knownKind) return; // Agent results must not inflate the HN event counters.
    if (kind === 'TOP') {
      if (!Array.isArray(event.items)) return;
      const items = event.items.filter(it => it?.id);
      items.forEach(it => this.track(it, ts));
      this.frontHistory.push({ ts, ranks: new Map(items.map((it, i) => [it.id, i + 1])) });
      this.frontHistory.sort((a, b) => a.ts - b.ts);
      if (ts >= this.frontTs) { this.front = items.map(it => it.id); this.frontTs = ts; }
      return;
    }
    const type = kind === 'PROFILE' ? 'profile' : kind === 'UPDATE' ? 'update'
      : item?.type === 'story' ? 'story' : item?.type === 'comment' ? 'comment' : 'other';
    this.events.push({ ts, type });
    if (!item) return;
    if (item.type !== 'comment') this.track(item, ts);
    if (kind === 'NEW' && visible(item)) {
      // Keep submissions separately: a busy comment feed must not evict new stories.
      if (item.type === 'story' && !this.submissions.has(item.id)) this.submissions.set(item.id, { ts, item });
      this.feed.set(item.id, { ts, item });
      if (this.feed.size > 250) {
        const oldest = [...this.feed].sort((a, b) => a[1].ts - b[1].ts)[0][0];
        this.feed.delete(oldest);
      }
    }
    if (!visible(item)) { this.submissions.delete(item.id); this.feed.delete(item.id); }
  }

  gain(story, now) {
    const history = story.history, last = history.at(-1);
    if (!last) return { points: 0, comments: 0, duration: 0 };
    let base = history[0];
    for (const sample of history) {
      if (sample.ts > now - WINDOW) break;
      base = sample;
    }
    return { points: last.points - base.points, comments: last.comments - base.comments,
      duration: Math.min(WINDOW, Math.max(0, now - base.ts)) };
  }

  snapshot(now = Date.now()) {
    const start = now - HOUR;
    this.events = this.events.filter(e => e.ts > start && e.ts <= now);
    for (const [key, ts] of this.seen) if (ts <= start) this.seen.delete(key);
    for (const map of [this.submissions, this.feed, this.subjects, this.emergingTopics]) for (const [key, entry] of map) if (entry.ts <= start) map.delete(key);
    for (const [id, story] of this.stories) {
      // Keep one baseline immediately before the rolling window, not an invented zero.
      while (story.history.length > 1 && story.history[1].ts <= now - WINDOW) story.history.shift();
      if (story.ts <= start && !this.front.includes(id)) this.stories.delete(id);
    }
    while (this.frontHistory.length > 1 && this.frontHistory[1].ts <= now - WINDOW) this.frontHistory.shift();
    const baseline = this.frontHistory[0]?.ranks;
    const rows = [...this.stories.values()].filter(s => visible(s.item)).map(s => ({
      item: s.item, changed: s.changed, gain: this.gain(s, now),
    }));
    const byId = new Map(rows.map(row => [row.item.id, row]));
    const rankLabels = store => {
      const counts = new Map();
      for (const [id, { labels }] of store) {
        if (!byId.has(id)) continue;
        for (const label of labels) counts.set(label, (counts.get(label) || 0) + 1);
      }
      return [...counts].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).slice(0, 12);
    };
    const front = this.front.flatMap((id, i) => byId.has(id) ? [{ ...byId.get(id), rank: i + 1,
      movement: baseline?.has(id) ? baseline.get(id) - i - 1 : null }] : []);
    const counts = { story: 0, comment: 0, update: 0, profile: 0, other: 0 };
    const bins = Array.from({ length: 60 }, () => ({ story: 0, comment: 0, update: 0, profile: 0, other: 0 }));
    const end = Math.floor(now / 60_000) * 60_000 + 60_000;
    // Include both partial edge seconds, then clip the display to the exact rolling minute.
    const secondStart = Math.floor((now - 60_000) / 1000) * 1000;
    const secondBins = Array.from({ length: 61 }, (_, i) => ({ ts: secondStart + i * 1000, count: 0 }));
    let recentTotal = 0;
    let recent = 0;
    for (const e of this.events) {
      counts[e.type]++;
      if (e.ts > now - 300_000) recent++;
      if (e.ts > now - 60_000) {
        secondBins[Math.floor((e.ts - secondStart) / 1000)].count++;
        recentTotal++;
      }
      const index = 59 - Math.floor((end - e.ts - 1) / 60_000);
      if (index >= 0 && index < 60) bins[index][e.type]++;
    }
    const domains = new Map();
    for (const { item } of front) {
      const name = sourceDomain(item.url);
      domains.set(name, (domains.get(name) || 0) + 1);
    }
    const submissions = [...this.submissions.values()].map(entry => ({ ...entry, item: this.stories.get(entry.item.id)?.item || entry.item }))
      .filter(entry => visible(entry.item)).sort((a, b) => b.ts - a.ts);
    return { now, front, counts, bins, binEnd: end, rate: Math.round(recent / 5),
      recentActivity: { bins: secondBins, total: recentTotal, peak: Math.max(0, ...secondBins.map(bin => bin.count)) },
      subjects: rankLabels(this.subjects), emergingTopics: rankLabels(this.emergingTopics),
      rising: rows.filter(r => r.item.type === 'story' && r.gain.points > 0).sort((a, b) => b.gain.points - a.gain.points || b.item.id - a.item.id),
      conversations: rows.filter(r => r.item.type === 'story' && r.gain.comments > 0).sort((a, b) => b.gain.comments - a.gain.comments || b.item.id - a.item.id),
      domains: [...domains].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])), submissions,
      feed: [...this.feed.values()].sort((a, b) => b.ts - a.ts), lastTs: this.lastTs };
  }
}
