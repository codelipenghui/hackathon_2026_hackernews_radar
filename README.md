# Alpha Radar

Real-time earnings trading on Kafka. SEC filings start an AI research team ([ai-berkshire](https://github.com/xbtlin/ai-berkshire)),
and a fast rules engine paper-trades only inside its verdict.

> Buffett decides **what**. Livermore decides **when**. The Turtles decide **how much** and **when to quit**.

```
edgar.py ─▶ filings ─▶ router.py ─▶ skill-requests ─▶ research.py (Claude Code + ai-berkshire) ─▶ research-verdicts ─┐
news.py ──▶ news                                                                                                    ▼
producer.py ▶ hn-events                                                    replay.py ─▶ ticks ─▶ trader.py ─▶ orders, fills, pnl
                                         dashboard.py: every topic ─SSE─▶ cockpit (/) and HN radar (/radar)
```

## Run

```sh
SEC_UA="AlphaRadar you@team.dev" docker compose up -d --build
KAFKA=localhost:9092 BERKSHIRE_DIR=~/ai-berkshire python research.py   # on a machine logged in to Claude Code
open http://localhost:8080        # pick a recorded day, press "Replay earnings day"
```

Record another day: `python record.py --list`, then `python record.py <TICKER> <ACCESSION> [--news news.json]`,
then `python research.py warm <TICKER> <ACCESSION>` to cache its verdict. Tests: `pytest`. Full stack: `python e2e.py data/replays/<file>.jsonl`.

Set up the research machine once (installs ai-berkshire's commands into its own checkout, not your global `~/.claude`):

```sh
git clone https://github.com/xbtlin/ai-berkshire ~/ai-berkshire
CLAUDE_COMMANDS_DIR=~/ai-berkshire/.claude/commands ~/ai-berkshire/scripts/install-claude-commands.sh
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
```

On a python.org Python for macOS, HTTPS calls (SEC, yfinance, RSS) fail with `CERTIFICATE_VERIFY_FAILED` until you
run `export SSL_CERT_FILE=$(.venv/bin/python -m certifi)` (or the installer's "Install Certificates.command").

Kafka listens on `127.0.0.1:9092` only (it has no auth, and `research.py` hands requests to Claude), so run
`research.py` on the machine that runs Docker.

Paper trading only. Not investment advice.

---

# HN Radar

A real-time Hacker News dashboard. The HN API is hosted on Firebase, and Firebase supports
**streaming**: send `Accept: text/event-stream` to any endpoint and you get Server-Sent Events
whenever that node changes. HN updates the nodes in batches about every 30s.

```
hacker-news.firebaseio.com          producer.py              Kafka (KRaft, 1 node)        dashboard.py          browser
  /v0/maxitem    (SSE) ─┐                                                                                    
  /v0/updates    (SSE) ─┼─▶ fetch item details ─▶  topic hn-events (7d retention) ─▶ consumer per tab ─SSE─▶ index.html
  /v0/topstories (SSE) ─┘                                                         (replays last 1h, then live)
```

## HN radar: run on its own

```sh
docker compose up -d --build kafka producer dashboard
open http://localhost:8080/radar
```

Kafka is also exposed on `localhost:9092` for your own consumers.

## Topic `hn-events`

One JSON message per change, keyed by item id or username:

| kind      | source                 | payload                                   |
|-----------|------------------------|-------------------------------------------|
| `new`     | `maxitem` moved        | `item`: new story / comment / job / poll  |
| `update`  | `updates.items`        | `item`: current state of a changed item   |
| `profile` | `updates.profiles`     | `id`: username whose profile changed      |
| `top`     | `topstories` changed   | `items`: the current top 30 stories       |

Every message has `ts` (ms). Items are stored as HN returns them, minus `kids`.

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic hn-events --from-beginning --max-messages 5
```

## Dashboard

- **Radar**: the 30 front-page stories. Distance from the center is the rank (#1 in the middle). The angle is fixed per story, so you can watch stories move in or out. Color goes from green to orange as the story gains points over 30 minutes. A ring pulses when a story gets new votes or comments. The outer band shows new submissions from the last 10 minutes.
- **Front page**: the current top 30 with rank change over the last 30 minutes, plus points gained.
- **Rising**: the stories that gained the most points in the last 30 minutes.
- **Activity**: events per minute over the last hour, split by type.
- **Live feed**: new stories and comments.

A new tab rebuilds all of this from the last hour of the topic. Each SSE event id is the Kafka
offset, so when the browser reconnects it picks up exactly where it left off.

## Known limits

- The dashboard assumes `hn-events` has a single partition, which is what Kafka auto-creates.
- If the producer restarts, items created while it was down are not backfilled.
