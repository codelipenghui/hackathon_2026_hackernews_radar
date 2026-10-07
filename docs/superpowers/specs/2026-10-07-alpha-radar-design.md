# Alpha Radar — design spec

**Date:** 2026-10-07 · **Event:** 1-day real-time pipeline hackathon · **Team:** 3 people (A, B, C)

## 1. Goal

Turn HN Radar (Hacker News → Kafka → browser) into a real-time trading pipeline that:

1. joins four live sources: **SEC earnings filings, stock prices, financial news, Hacker News**;
2. starts a **deep AI research run** ([ai-berkshire](https://github.com/xbtlin/ai-berkshire), MIT) when a company publishes its earnings;
3. lets a **fast rules engine** trade *only inside* the research verdict, using famous traders' timing and risk rules;
4. **scores every decision live** (paper P&L) on a dashboard.

**Pitch:** *"Buffett decides what. Livermore decides when. The Turtles decide how much and when to quit. All of it on one Kafka stream you can replay."*

**Success = a 3-minute demo** that runs end to end from a replayed real earnings day, with no manual steps after "start replay", and survives a slow or failed research run.

### Non-goals (cut for the 1-day build)

- Real-money or broker-connected trading. Paper only.
- The HN prediction market, X/Twitter, global news (GDELT), crypto, LLM role-play personas.
- Multi-partition topics. Every topic has one partition, as today.
- Accounts or auth. The dashboard is local and read-only.

### Stretch (only after the 8:00 code freeze build is solid)

S1 Haiku headline classifier on news and HN titles · S2 live price feed during market hours · S3 `/news-pulse` triggered by price moves or HN spikes · S4 a standalone Turtle bot with its own P&L compared to Berkshire+fast.

## 2. Architecture

```
 SOURCES                         SLOW BRAIN                               FAST BRAIN                       OUTPUT
 edgar.py  ──▶ filings ──┐
 news.py   ──▶ news    ──┤──▶ router.py ──▶ skill-requests ──▶ research.py ──▶ research-verdicts ──┐
 producer.py ▶ hn-events ┘    (rules, watchlist,               (claude -p + ai-berkshire,         │
 replay.py ──▶ ticks  ─────────dedupe, budget)                  JSON verdict, cache fallback)      ▼
           (and filings/news                                                            trader.py ──▶ orders, fills, pnl
            from a recorded day)                                                        (Livermore entry, Turtle stop
                                                                                         & sizing, paper broker, scorer)
                                                                                                   │
                                        dashboard.py: all topics ─SSE─▶ cockpit.html (+ existing radar)
```

- **Kafka is the system of record.** Every service is a consumer → logic → producer. State is in memory. The dashboard rebuilds its view by reading topics from the start of the selected run; the router, trader and research services start from the end of their topics (see §5). A run's start is found with Kafka's `offsets_for_times` (record timestamps are produce time): for `replay-<unix-seconds>`, seek to that second; for `live`, seek to now − 1 hour.
- **Run isolation:** every message carries `run`. Live data uses `run: "live"`. Each replay uses `run: "replay-<unix-seconds>"`. Every stateful service keys its state by `run`, and the dashboard shows one run at a time (a dropdown). This lets live HN and a replayed earnings day share topics without mixing.
- **Time:** every message carries `ts` = **event time** in epoch ms (for a replay, the original historical time). Logic uses `ts`, never the wall clock, so the rules behave identically at 1× and 60× replay speed.

### Files (flat layout, matching the current repo)

| File | Owner | New / changed |
|---|---|---|
| `common.py` | A (hour 1) | new: Kafka producer/consumer helpers, `emit(topic, key, msg)`, watchlist loader, ticker↔CIK↔name map |
| `producer.py` | — | changed: adds `run: "live"` to every message (one line) |
| `edgar.py` | A | new: live SEC filings poller |
| `news.py` | A | new: newswire RSS poller |
| `record.py` | A | new: builds a replay file for one earnings day |
| `replay.py` | A | new: plays a replay file into Kafka at N× speed |
| `router.py` | B | new: trigger rules |
| `research.py` | B | new: runs ai-berkshire, publishes verdicts (runs on a **host**, not in Docker) |
| `trader.py` | C | new: fast brain + paper broker + scorer |
| `dashboard.py` | C | changed: streams multiple topics; `POST /replay` |
| `cockpit.html` | C | new: trading cockpit (the radar stays at `/radar`) |
| `watchlist.json` | A | new: tickers + company names + aliases |
| `data/replays/*.jsonl` | A | new: committed recordings, so the demo never needs yfinance or EDGAR |
| `data/verdicts/*.json` | B | new: committed cached verdicts |

## 3. Topics and message contracts (lock these in hour 1)

All topics: 1 partition, JSON values, UTF-8, key as given. All messages have `run` (string) and `ts` (int, epoch ms, event time). Prices are floats in USD.

### `filings` — key: ticker
```json
{"run":"live","ts":1758834000000,"ticker":"MU","cik":"0000723125","form":"8-K",
 "items":["2.02","9.01"],"accession":"0000723125-25-000041",
 "url":"https://www.sec.gov/Archives/edgar/data/723125/000072312525000041/",
 "title":"MICRON TECHNOLOGY INC - 8-K"}
```
`items` is only meaningful for 8-K. Item **2.02** = Results of Operations (earnings).

### `ticks` — key: ticker
```json
{"run":"replay-1759840000","ts":1758834300000,"ticker":"MU","price":157.42,"volume":182300,"session":"post"}
```
One message per bar close (5-minute bars in replay). `session` ∈ `pre | regular | post`. `volume` is the bar's volume.

### `news` — key: ticker (or `"_"` if unmatched)
```json
{"run":"live","ts":1758834060000,"ticker":"MU","source":"globenewswire",
 "headline":"Micron Technology, Inc. Reports Results for the Fourth Quarter ...","url":"https://..."}
```

### `hn-events` — unchanged, plus `"run":"live"`
The dashboard and router match HN story titles against watchlist names/aliases client-side and server-side respectively. No new HN topic.

### `skill-requests` — key: ticker
```json
{"run":"replay-1759840000","ts":1758834000000,"request_id":"MU-0000723125-25-000041",
 "ticker":"MU","skill":"earnings-review","args":"MU latest",
 "reason":"8-K Item 2.02","trigger":{"topic":"filings","accession":"0000723125-25-000041"}}
```

### `research-verdicts` — key: ticker
Two kinds, distinguished by `status`:
```json
{"run":"replay-1759840000","ts":1758834000000,"request_id":"MU-0000723125-25-000041",
 "ticker":"MU","status":"started","skill":"earnings-review"}
```
```json
{"run":"replay-1759840000","ts":1758834000000,"request_id":"MU-0000723125-25-000041",
 "ticker":"MU","status":"done","source":"live","skill":"earnings-review",
 "verdict":"PASS","score":4.3,
 "masters":{"buffett":4.4,"munger":3.5,"duan":3.7,"lilu":4.0},
 "buy_low":140.0,"buy_high":160.0,"target":195.0,
 "red_lines":["gross margin below 40%","HBM share loss to Samsung"],
 "summary":"Record HBM revenue; guidance above consensus; ...",
 "report_path":"data/reports/MU-0000723125-25-000041.md","duration_s":312}
```
- `status` ∈ `started | done | failed`. `source` ∈ `live | cache`.
- `verdict` ∈ `PASS | GRAY | FAIL`. `score` is 0–5. `buy_low`/`buy_high` may be `null` (then the fast brain never opens a position).
- `ts` of a verdict = `ts` of the triggering filing, so it lines up on the replay timeline.

### `orders` — key: ticker
```json
{"run":"...","ts":...,"order_id":"o-17","ticker":"MU","side":"BUY","qty":120,
 "rule":"livermore_pivot","reason":"broke 30-min high 158.10 on 1.8x volume; inside band 140–160",
 "request_id":"MU-0000723125-25-000041"}
```
`side` ∈ `BUY | SELL`. `rule` ∈ `livermore_pivot | turtle_stop | target_hit | thesis_fail | end_of_run`.

### `fills` — key: ticker
```json
{"run":"...","ts":...,"order_id":"o-17","ticker":"MU","side":"BUY","qty":120,"price":158.25}
```

### `pnl` — key: run
```json
{"run":"...","ts":...,"cash":81010.0,"equity":100412.5,"realized":0.0,"unrealized":412.5,
 "positions":{"MU":{"qty":120,"avg":158.25,"last":161.69,"stop":151.9}}}
```
Emitted on every fill and at most once per ticker bar.

## 4. Components

### 4.1 Sources (Person A)

**`watchlist.json`** — about 10 tickers, taken from ai-berkshire's `data/watchlist.json` (US AI chips/apps/infra), plus the demo ticker:
```json
{"MU":{"cik":"0000723125","names":["Micron"],"aliases":["HBM"]}, "NVDA":{...}}
```
The CIK comes from `https://www.sec.gov/files/company_tickers.json` (fetched once by hand; stored in the file).

**`edgar.py`** (live) — every 60 s, reads the latest-filings Atom feed
`https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&count=100&output=atom`,
keeps entries whose CIK is in the watchlist, parses item numbers from the entry summary, and emits to `filings` once per accession. Also polls `10-Q` and `10-K`.
- SEC rules: send `User-Agent: "AlphaRadar hackathon <team email>"`; stay well under 10 requests/s.

**`news.py`** (live) — every 60 s, polls GlobeNewswire, PR Newswire and Business Wire RSS feeds. A headline matches a ticker if it contains one of the watchlist names. Dedupes by URL. Matched and unmatched headlines are both emitted (unmatched with key `"_"`; the dashboard hides them by default).

**`record.py <TICKER> <ACCESSION>`** — builds `data/replays/<TICKER>-<date>.jsonl`, one JSON message per line, sorted by `ts`, with `"run"` left blank:
1. the `filings` message, from `https://data.sec.gov/submissions/CIK##########.json` (the `items` field holds `2.02`);
2. `ticks` from `yfinance` 5-minute bars with `prepost=True`, trimmed to keep the demo short: from 30 min before the filing to 2 h after it, plus 08:00–16:00 ET of the first regular session after the filing (about 130 bars; yfinance keeps about 60 days of 5-minute bars);
3. `news` headlines for that window, written by hand (2–4 lines, real headlines and URLs) — at least one negative headline after the entry, to drive the demo's "bad news" moment.

**`replay.py <file> [--speed 300]`** (300× ≈ 1 s per 5-minute bar, so a recorded day plays in about 2 minutes) — assigns `run = "replay-<now>"`, then emits each line to its topic, sleeping `(ts_next − ts) / speed` between messages (capped at 5 s, so overnight gaps don't stall the demo). Also exposed via `POST /replay` on the dashboard.

**Demo ticker choice (hour 1):** a watchlist company with an 8-K Item 2.02 in the last 60 days and a post-earnings move of ≥5%. Late-September reporters such as **MU** are candidates to verify. Record **two** days, so there's a backup.

### 4.2 Slow brain (Person B)

**`router.py`** — consumes `filings`, `news` and `hn-events`; emits `skill-requests`. Rules (first match wins):

| # | Condition | Skill | Args |
|---|---|---|---|
| R1 | `filings`: watchlist ticker, 8-K containing item `2.02`, or form `10-Q`/`10-K` | `earnings-review` | `"<TICKER> latest"` |
| R2 (stretch S3) | HN: ≥3 front-page or new stories mentioning a ticker within 30 min | `news-pulse` | `"<TICKER> 7d"` |

Guards: at most **1** request per ticker per `run` (this also dedupes `request_id` = `<TICKER>-<accession>` within a run; a new replay of the same filing triggers again and hits the cache). `research.py` runs at most **2** research jobs at a time (a 2-worker pool), since it is the service that knows when one finishes.

**`research.py`** — runs on a team laptop where Claude Code is logged in and ai-berkshire is cloned and its commands are installed (`scripts/install-claude-commands.sh`). For each `skill-requests` message:

1. Emit `status: started`.
2. **Cache check:** if `data/verdicts/<request_id>.json` exists **and** `run` is a replay, wait `CACHE_DELAY_S` (default 20 s, so the "researching…" card is visible) and then emit it with `source: "cache"`. Otherwise run live.
3. **Live run:** from the ai-berkshire directory, execute
   `claude -p "/<skill> <args>" --output-format json --allowedTools "WebSearch,WebFetch,Read,Write,Bash,Task"` with a **10-minute timeout**. The skill writes its report under `reports/`.
4. **Verdict extraction:** a second, short `claude -p` call with the report text and the instruction *"Return only JSON matching this schema: {verdict, score, masters, buy_low, buy_high, target, red_lines, summary}. Write summary and red_lines in English."* Validate the fields; retry once on invalid JSON.
5. Copy the report to `data/reports/<request_id>.md`, save the verdict to `data/verdicts/<request_id>.json` (so later replays hit the cache), and emit `status: done`.
6. On timeout or failure: fall back to the cache if present (`source: cache`); otherwise emit `status: failed`.

**Before the demo:** run the live path once for each recorded demo ticker and commit the resulting verdicts. This also measures real research time for the pitch.

### 4.3 Fast brain, broker and scorer (Person C) — `trader.py`

One process consumes `research-verdicts` and `ticks` and emits `orders`, `fills` and `pnl`. The broker and scorer run in the same process, so it never needs to read its own output. State per `run`: verdict per ticker, bar history, positions, cash (starts at **$100,000**).

In the 1-day build only replays carry `ticks` (live prices are stretch S2), so the `live` run shows filings, news and verdicts but never trades.

**Thesis gate:** no position opens without a `done` verdict with `verdict == "PASS"` and a non-null buy band. A later `FAIL` for a held ticker → `SELL` all with `rule: thesis_fail`.

**Entry — Livermore pivotal point** (`livermore_pivot`):
- Pivot = highest price of the first **6 bars (30 min)** of the first regular session after the filing.
- Buy when a later bar closes **above the pivot**, with bar volume ≥ **1.5×** the average of the previous 12 bars, **and** price is within `[buy_low, buy_high]`.
- Only one entry per ticker per run.

**Size — Turtle volatility sizing:**
- `N` = ATR(14) over the bars so far (true range from consecutive bar closes, since ticks carry only close; good enough at 5-minute granularity).
- `qty = floor(0.01 × equity / (2 × N))` — hitting the stop loses about 1% of the account. Capped so the position is ≤ 25% of equity.

**Exit — Turtle stop and target:**
- Stop = `entry − 2N`, fixed at entry (`turtle_stop`).
- Take profit when price ≥ `target`, or `buy_high × 1.10` if `target` is null (`target_hit`).
- At the last tick of a replay file, close everything (`end_of_run`) so the scorer shows realized P&L. The replayer marks the last message with `"last": true`.

**Paper broker:** fills every order on the **same tick** that caused it, at `price × (1 ± 0.0005)` (5 bps slippage, worse for the trader). Emits `fills`, updates cash and positions.

**Scorer:** after each fill and at most once per ticker bar, emits `pnl` with cash, equity, realized and unrealized P&L, and per-position stop. The buy-and-hold benchmark (from the first tick after the filing) is computed in the cockpit from `ticks`, next to the strategy's return.

### 4.4 Dashboard (Person C) — `dashboard.py` + `cockpit.html`

**Server:**
- `GET /events?topics=a,b,c` — one Kafka consumer assigned to partition 0 of each listed topic; replays the last hour (live) or from the start of the selected run; SSE `data` = `{"topic": ..., ...message}`. The SSE `id` is a compact JSON map `{topic: offset}` so `Last-Event-ID` resumes every topic exactly. One connection per tab (browsers allow only ~6 HTTP/1.1 connections per origin).
- `POST /replay?file=<name>&speed=60` — starts `replay.py` in a background thread; returns the new `run`.
- `GET /` → `cockpit.html`; `GET /radar` → the existing `index.html` unchanged.

**`cockpit.html` panels:**
1. **Run selector** (`live` plus replays started from this dashboard, newest first) **+ "Replay earnings day" button**, with speed.
2. **Price chart** (canvas, like the existing radar code): price line, buy band shaded, pivot line, stop line, trade markers. Hovering a marker shows `rule` and `reason`.
3. **Research card:** `started` → "🔬 4 masters researching MU… 2:41"; `done` → verdict badge, score, four master scores, band, target, red lines, summary, link to the report, and `live`/`cache` source.
4. **Decision timeline:** filing → research started → verdict → each order/fill, with the source topic and timestamp of each.
5. **P&L:** equity curve vs. buy-and-hold, positions table.
6. **News feed:** `news` headlines and HN stories that mention watchlist names, labelled by source. In the 1-day build news is **display only**; the trader acts on price, verdicts and stops (acting on headlines is stretch S1).
7. **Side panel:** a compact link/preview of the live HN radar, to show the pipeline is live 24/7.

## 5. Error handling (demo-safety first)

| Failure | Behaviour |
|---|---|
| Live research slow, failing, or Claude login problem | Cache fallback (4.2 step 6); the card shows `source: cache`. A replay never depends on a live research run. |
| yfinance / EDGAR / RSS down on demo day | Replays use only committed `data/replays/*.jsonl`; live sources just log and retry every 60 s. |
| Verdict JSON invalid | Retry extraction once; then `failed`, and the trader never opens a position. |
| A service crashes mid-demo | `restart: unless-stopped`. Router, trader and research start from the end of their topics (rebuilding would re-send duplicate orders), so after a restart **press Replay again** (a new run). The dashboard rebuilds its view from the run's start, so reloading a tab is always safe. |
| Two people press "Replay" | Each press is a new `run`; the dashboard follows the newest unless one is pinned. |
| Market closed during the demo | Not an issue: replays carry their own historical time. |

## 6. Testing

Small and focused; no time for more in a single day.

- **Unit (pytest), pure functions only:**
  - `trader`: pivot detection, the volume filter, ATR, position size, stop/target exits, the thesis gate (no verdict → no trade; FAIL → sell).
  - `router`: R1 matches 8-K with 2.02 and ignores 8-K without it; dedupe; concurrency cap.
  - `research`: verdict validation accepts the example in §3 and rejects missing/invalid fields.
  - `edgar`: item parsing from a saved Atom entry.
- **Contract fixtures:** `tests/fixtures/contracts.json` holds the examples in §3, keyed by topic. Each person's service has a test that consumes or produces exactly these. These are also the fake messages used from hour 1 to hour 6.
- **End-to-end (integration hour):** `replay.py data/replays/<demo>.jsonl --speed 600` with a cached verdict must produce ≥1 `BUY` fill, one exit, and a final `pnl` with zero open positions. Run it after every merge from 6:00 onwards.

## 7. Plan for the day

| Time | Everyone / A / B / C |
|---|---|
| 0:00–1:00 | **All:** confirm §3 contracts, write `tests/fixtures`, pick the demo ticker. A: `common.py`, `watchlist.json`. |
| 1:00–6:00 | **A:** `record.py` (record both demo days first, then commit), `replay.py`, `edgar.py`, `news.py`. **B:** `router.py`, `research.py`, first live ai-berkshire run on the demo ticker (start early: it takes minutes). **C:** `trader.py` with fixtures, then `dashboard.py` + `cockpit.html`. |
| 6:00–8:00 | Integration: real topics end to end; e2e replay after every merge. |
| **8:00** | **Code freeze.** Fixes only. |
| 8:00–10:00 | Rehearse twice, slides (including the roadmap), record a backup video of a full replay. |

## 8. Demo script (about 3 minutes)

1. Open `/radar`: HN flowing live — "this pipeline is real time, 24/7".
2. Switch to the cockpit, press **Replay earnings day** (300×). The 8-K Item 2.02 filing appears in the timeline.
3. Research card: "🔬 4 masters researching MU…". Explain: ai-berkshire, started by the event.
4. Verdict lands: PASS 4.3/5, buy band shown on the chart.
5. Next morning in the replay: the price breaks the 30-minute pivot on high volume → **BUY** marker: "Livermore pivot · Turtle 1% risk · Berkshire PASS 4.3".
6. A negative headline lands in the news feed; price falls and hits the 2N stop, or reaches the target → exit marker. P&L vs. buy-and-hold updates.
7. Roadmap slide: more sources (X/Bluesky, GDELT, crypto), Haiku headline classifier, more trader personas, real broker paper trading, multi-partition scaling.

## 9. Open items to settle in hour 1

- The demo ticker and the backup ticker (criteria in §4.1).
- Whose laptop runs `research.py` with Claude Code and ai-berkshire installed.
- The team email for the SEC `User-Agent`.

## 10. Addendum (2026-10-07, after first demo run)

Agreed with the user after seeing the live cockpit. Supersedes earlier sections where they conflict.

### 10.1 Live mode
- `prices.py` publishes yfinance 5-minute bars for the watchlist to `ticks` (`run: "live"`), each completed bar once; the
  first poll sends the current/last session. Live view keeps 24 h of market data (HN stays at 1 h).
- `POST /research?ticker=T` ("Research now") publishes T's latest earnings 8-K to `filings` with `on_demand: true`;
  the router allows it once per filing.

### 10.2 Two research tiers
`/earnings-review` takes 10–30+ minutes, too slow to trade on. Every earnings request now yields two verdicts, both
on `research-verdicts`, distinguished by a new required field `tier`:
- `quick` — one Claude session (Sonnet, WebSearch/WebFetch only, ≤5 min) applying the four masters to the filing and
  returning the verdict JSON directly. Tradeable; cached as `data/verdicts/<request_id>.quick.json`.
- `deep` — `/earnings-review` in the background (one at a time, ≤45 min), told to wait for its agents and write the
  report; reports are found recursively under ai-berkshire's `reports/`. Replaces the quick verdict when done;
  cached as `data/verdicts/<request_id>.json`.
Replays use the deep cache if present, else the quick cache, else run quick live; a missing deep verdict is scheduled.

### 10.3 News-triggered re-verdict
- `triage.py` (host, next to `research.py`) reads `news`, `hn-events` and `research-verdicts`. A headline about a
  ticker that has a `done` verdict in that run (and no research running, and no re-check in the last 30 min of event
  time) is classified by Claude Haiku with no tools: `{material, direction, reason}`. Every decision goes to a new
  topic `news-triage`; material ones also publish a `skill-requests` message:
  `skill: "news-recheck"`, `args: "<T> 3d"`, `request_id: "<T>-news-<8 hex>"`, `prior: <current verdict fields>`.
- `research.py` runs a quick-tier re-check (`tier: "news"`): it searches recent news itself (the headline is never put
  in a prompt that has tools) and returns the updated verdict (same schema), given the prior verdict on stdin.
- Trader: a news verdict is a verdict. FAIL sells (`thesis_fail`), GRAY holds and blocks new entries, PASS with a new
  band changes future entries.

### 10.4 Cockpit
- **Live | Replay** switch. Live: ticker, range (1D/5D/1M), Research now. Replay: recorded day, speed, ▶, run picker.
- 1D = the 5-minute stream; 5D (15-minute bars) and 1M (daily bars) come from `GET /history?ticker=T&range=5d|1mo`
  (yfinance, cached 60 s), overlays drawn by time. Price axis on the left, last-price tag on the right.
- Research card shows the current verdict with its tier (⚡ quick / 🔬 deep / 📰 news) and what is still running;
  the timeline shows `news-triage` decisions.
