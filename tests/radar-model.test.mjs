import test from 'node:test';
import assert from 'node:assert/strict';
import { RadarModel, HOUR, WINDOW } from '../radar-model.mjs';

const now = 1_800_000_000_000;
const story = (id, score = 10, descendants = 3, extra = {}) => ({ id, type: 'story', title: `Story ${id}`, score, descendants, url: 'https://example.com/article', ...extra });
const top = (model, ts, items) => model.ingest({ kind: 'TOP', ts, items });
const update = (model, ts, item) => model.ingest({ kind: 'UPDATE', ts, item });

test('gains use the last baseline at or before the window, or the first observation for young stories', () => {
  const model = new RadarModel();
  update(model, now - 50 * 60_000, story(1, 5, 1));
  update(model, now - 31 * 60_000, story(1, 20, 4));
  update(model, now - 29 * 60_000, story(1, 30, 7));
  update(model, now - 1000, story(1, 42, 12));
  update(model, now - 120_000, story(2, 100, 30));
  update(model, now - 1000, story(2, 103, 35));
  const data = model.snapshot(now);
  assert.deepEqual(data.rising.map(r => [r.item.id, r.gain.points]), [[1, 22], [2, 3]]);
  assert.deepEqual(data.conversations.map(r => [r.item.id, r.gain.comments]), [[1, 8], [2, 5]]);
  assert.equal(data.rising[0].gain.duration, WINDOW);
  assert.equal(data.rising[1].gain.duration, 120_000);
});

test('late rows do not roll back the latest story or front-page ordering', () => {
  const model = new RadarModel();
  top(model, now - WINDOW - 1000, [story(1), story(2)]);
  top(model, now - 1000, [story(2, 50, 10), story(1, 20, 7)]);
  top(model, now - 5000, [story(1, 15, 5), story(2, 30, 7)]);
  update(model, now - 3000, story(2, 40, 8));
  const data = model.snapshot(now);
  assert.deepEqual(data.front.map(r => [r.item.id, r.item.score, r.movement]), [[2, 50, 1], [1, 20, -1]]);
  assert.equal(data.front[0].gain.points, 40);
});

test('duplicate replay events are counted once and TOP snapshots do not inflate traffic', () => {
  const model = new RadarModel();
  const event = { kind: 'NEW', ts: now - 1000, item: story(1) };
  model.ingest(event); model.ingest(event);
  top(model, now - 500, [story(1)]);
  model.ingest({ kind: 'PROFILE', ts: now - 1000, user: 'one' });
  model.ingest({ kind: 'PROFILE', ts: now - 1000, user: 'two' });
  const data = model.snapshot(now);
  assert.equal(data.counts.story, 1);
  assert.equal(data.counts.profile, 2);
  assert.equal(data.submissions.length, 1);
  assert.equal(data.bins.reduce((n, bin) => n + bin.story + bin.profile, 0), 3);
});

test('rolling traffic and submissions expire, and busy comments do not evict submissions', () => {
  const model = new RadarModel();
  model.ingest({ kind: 'NEW', ts: now - HOUR - 1, item: story(1) });
  model.ingest({ kind: 'NEW', ts: now - 600_000, item: story(2) });
  for (let i = 0; i < 260; i++) model.ingest({ kind: 'NEW', ts: now - 1000 + i, item: { id: i + 100, type: 'comment', text: 'Hello' } });
  const data = model.snapshot(now);
  assert.equal(data.counts.story, 1);
  assert.equal(data.counts.comment, 260);
  assert.equal(data.feed.length, 250);
  assert.deepEqual(data.submissions.map(e => e.item.id), [2]);
  assert.equal(data.rate, 52);
});

test('source counts use current front-page stories, including HN text posts, not all observed stories', () => {
  const model = new RadarModel();
  top(model, now - 1000, [story(1), story(2, 5, 2, { url: 'https://www.example.com/2' }), story(3, 8, 4, { url: null })]);
  update(model, now - 500, story(4, 100, 20, { url: 'https://unlisted.com' }));
  assert.deepEqual(model.snapshot(now).domains, [['example.com', 2], ['Hacker News', 1]]);
});

test('deletions are excluded and submissions display their latest observed score', () => {
  const model = new RadarModel();
  model.ingest({ kind: 'NEW', ts: now - 6000, item: story(1) });
  model.ingest({ kind: 'NEW', ts: now - 5000, item: story(2) });
  update(model, now - 1000, story(1, 40, 20));
  update(model, now - 500, story(2, 10, 3, { deleted: true }));
  const data = model.snapshot(now);
  assert.deepEqual(data.submissions.map(e => [e.item.id, e.item.score]), [[1, 40]]);
  assert.ok(!data.rising.some(row => row.item.id === 2));
});

test('one observation does not invent growth and empty views remain valid', () => {
  const model = new RadarModel();
  const empty = model.snapshot(now);
  assert.equal(empty.lastTs, 0); assert.equal(empty.front.length, 0); assert.equal(empty.rate, 0);
  update(model, now - 1000, story(1, 900, 500));
  const data = model.snapshot(now);
  assert.equal(data.rising.length, 0); assert.equal(data.conversations.length, 0);
});

test('agent labels are counted once per observed story without inflating HN traffic', () => {
  const model = new RadarModel();
  top(model, now - 5000, [story(1), story(2)]);
  model.ingest({ kind: 'UPDATE', ts: now - 4000, item: story(1), story_id: 1, subjects: ['AI', 'AI', ' Rust ', null] });
  model.ingest({ kind: 'ANALYSIS', ts: now - 3000, story_id: 2, subjects: ['AI'], emerging_topics: ['Quantization', 'Quantization'] });
  model.ingest({ kind: 'ANALYSIS', ts: now - 2000, story_id: 99, subjects: ['Unobserved'], emerging_topics: ['Unobserved'] });
  const data = model.snapshot(now);
  assert.deepEqual(data.subjects, [['AI', 2], ['Rust', 1]]);
  assert.deepEqual(data.emergingTopics, [['Quantization', 1]]);
  assert.equal(data.counts.update, 1);
  assert.equal(Object.values(data.counts).reduce((a, b) => a + b, 0), 1);
});

test('analysis keeps the latest result, can be cleared, and expires with the observed window', () => {
  const model = new RadarModel();
  top(model, now - 5000, [story(1)]);
  model.ingest({ kind: 'ANALYSIS', ts: now - 2000, story_id: 1, subjects: ['Rust'], emerging_topics: ['Compilers'] });
  model.ingest({ kind: 'ANALYSIS', ts: now - 4000, story_id: 1, subjects: ['Outdated'] });
  assert.deepEqual(model.snapshot(now).subjects, [['Rust', 1]]);
  model.ingest({ kind: 'ANALYSIS', ts: now - 1000, story_id: 1, subjects: [] });
  assert.deepEqual(model.snapshot(now).subjects, []);
  assert.deepEqual(model.snapshot(now + HOUR).emergingTopics, []);
});
