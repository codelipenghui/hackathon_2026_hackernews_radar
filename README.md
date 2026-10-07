# HN Radar

A real-time Hacker News dashboard. The HN API is hosted on Firebase, and Firebase supports
**streaming**: send `Accept: text/event-stream` to any endpoint and you get Server-Sent Events
whenever that node changes. HN updates the nodes in batches about every 30s.

```
HN Firebase API ──SSE──▶ producer.py ──Avro──▶ StreamNative Cloud: Kafka topic hn-events + Schema Registry
                                                                    │
                                                                    ▼
browser ◀──SSE── dashboard.py ◀──SQL── RisingWave: Kafka source "hn-events" ─▶ materialized view "hn-events_mv"
```

It has two parts that run independently:

- **Producer** (`producer.py`): follows the HN streams and publishes every change to Kafka. Run exactly one, anywhere.
- **Dashboard** (`dashboard.py`): serves the page and streams the events from RisingWave to your browser. Anyone can run it.

## Run the dashboard

You need Docker, or Python 3.10+, and a RisingWave login that can read the view. Ask the project owner for a
read-only one.

```sh
git clone https://github.com/codelipenghui/hackathon_2026_hackernews_radar.git
cd hackathon_2026_hackernews_radar
cp .env.example .env    # fill in RISINGWAVE_URL; .env is git-ignored
docker compose up -d dashboard
# then open http://localhost:8080
```

Stop it with `docker compose stop dashboard`. Without Docker: `pip install "psycopg[binary]"`, then run
`python dashboard.py` with `RISINGWAVE_URL` set.

## Run the producer

Run exactly one producer, anywhere. Two would publish every event twice, so stop the old one when you
move it. It needs the four StreamNative settings listed under [Configuration](#configuration).

- **A machine with Docker:** fill them in `.env`, then run `docker compose up -d producer`.
- **A container platform** (Railway, Fly.io, Render, ...): deploy this repo's Dockerfile, which runs the producer by default, and set the four settings as secrets. The producer doesn't listen on a port, so deploy it as a background worker.

When it's working, its logs (`docker compose logs -f producer`) show the schema id on start, then one line
per HN batch, e.g. `updates: 77 items, 23 profiles`.

## Configuration

Each part reads its settings from environment variables. With Docker Compose, put them in `.env`, copied
from `.env.example`. `.env` is git-ignored. Never put real values in `.env.example`, because it's committed.

| Setting               | Used by   | Required | What                                                                                        |
|-----------------------|-----------|----------|---------------------------------------------------------------------------------------------|
| `RISINGWAVE_URL`      | dashboard | yes      | Connection string of a login that can read the view, e.g. `postgresql://USER:PASSWORD@HOST:4566/dev` |
| `RISINGWAVE_MV`       | dashboard | no       | Name of the materialized view. Default `hn-events_mv`                                       |
| `PORT`                | dashboard | no       | HTTP port when run without Compose. Default `8080`                                          |
| `KAFKA_SERVICE_URL`   | producer  | yes      | Kafka Service URL of the StreamNative cluster                                               |
| `SCHEMA_REGISTRY_URL` | producer  | yes      | Schema Registry URL                                                                         |
| `JWT_TOKEN`           | producer  | yes      | API key of a service account with produce permission on the topic and the `schema-writer` role |
| `KAFKA_USERNAME`      | producer  | yes      | The service account the API key belongs to: the `userName` in the console's client example  |

If a required setting is missing, the service stops at startup with `<NAME> is not set (see .env.example)`.

## Deploy Agents (StreamNative Agent Engine)

Two Claude 3.5 Sonnet agents analyze HN events in real-time:

**Prerequisites**
- Go 1.25+ (for building ork CLI)
- StreamNative Cloud access to Agent Engine workspace

**Setup**

1. Install/build ork CLI v0.6.0+
   ```bash
   # macOS with Go 1.25+
   git clone https://github.com/orca-ae/orca-cli
   cd orca-cli
   go install ./cmd/ork
   ```

2. Get Agent Engine endpoint from StreamNative console
   ```bash
   snctl -O o-1a54s get workspace <workspace-name> -o jsonpath='{.status.serviceEndpoints[?(@.type=="external")].dnsName}'
   ```

3. Create agents
   ```bash
   export ORCA_BASE_URL=https://<agent-engine-endpoint>
   export ORCA_ACCESS_TOKEN=<your-api-key>
   
   # Subject Extractor: identifies technical topics in titles
   ork agent create \
     --name hn-subject-extractor \
     --model claude-3-5-sonnet-20241022 \
     --registry-url $ORCA_BASE_URL \
     --access-token "$ORCA_ACCESS_TOKEN" \
     --system "Extract technical subjects (AI, LLM, Rust, Python, DevOps, etc.) from HN story titles. Respond with JSON: {\"story_id\": <int>, \"title\": \"<title>\", \"subjects\": [<strings>]}"
   
   # Topic Analyzer: finds emerging topics in comments
   ork agent create \
     --name hn-topic-analyzer \
     --model claude-3-5-sonnet-20241022 \
     --registry-url $ORCA_BASE_URL \
     --access-token "$ORCA_ACCESS_TOKEN" \
     --system "Analyze HN comments for emerging topics (model compression, quantization, tools, methodologies). Respond with JSON: {\"story_id\": <int>, \"title\": \"<title>\", \"emerging_topics\": [<strings>], \"ts\": <timestamp>}"
   ```

4. Create environment and sessions
   ```bash
   # Environment
   ork agent environments create \
     --name hn-radar-env \
     --registry-url $ORCA_BASE_URL \
     --access-token "$ORCA_ACCESS_TOKEN" -o json | jq -r '.id' > env_id.txt
   
   # Sessions (get agent IDs from create output above)
   ork agent sessions create \
     --agent <subject-extractor-id> --agent-version 1 \
     --environment-id $(cat env_id.txt) \
     --title "HN Subject Extraction" \
     --registry-url $ORCA_BASE_URL \
     --access-token "$ORCA_ACCESS_TOKEN"
   
   ork agent sessions create \
     --agent <topic-analyzer-id> --agent-version 1 \
     --environment-id $(cat env_id.txt) \
     --title "HN Topic Analysis" \
     --registry-url $ORCA_BASE_URL \
     --access-token "$ORCA_ACCESS_TOKEN"
   ```

**Agents Deployed**
- hn-subject-extractor (ID: agt_5017ZCP32NVMF970W3H3)
- hn-topic-analyzer (ID: agt_EF5DQG4F7H4YXS9HFWCF)
- Environment: hn-radar-env (ID: env_MM8SNVRL2ZZYRLJE0E5W)
- Sessions: Subject extraction (ses_3NXDES7WP15KH0CPP33C), Topic analysis (ses_3FXJW4X2D56SWN97FVD7)

## Setup (project owner, once)

1. **StreamNative Cloud:** a Kafka cluster with the topic `hn-events`, and a service account with produce permission on it plus the `schema-writer` role. The producer registers the schema on start.
2. **RisingWave:** a Kafka source on `hn-events` that decodes the Avro through the schema registry, and the view the dashboard reads:
   ```sql
   CREATE MATERIALIZED VIEW "hn-events_mv" AS SELECT * FROM "hn-events";  -- "hn-events": the Kafka source
   ```
3. **A read-only RisingWave login** that can select from the view, to share with dashboard users.

## Topic `hn-events`

- **Key:** a plain UTF-8 string: the item id, the username, or `top`.
- **Value:** Avro in the Confluent wire format. The schema is [`hn_event.avsc`](hn_event.avsc), registered as subject `hn-events-value` with BACKWARD compatibility.

Every record is an `HnEvent` with `kind`, `ts` (`timestamp-millis`), and one field that depends on the kind:

| kind      | source                 | field set                                       |
|-----------|------------------------|-------------------------------------------------|
| `NEW`     | `maxitem` moved        | `item`: new story / comment / job / poll        |
| `UPDATE`  | `updates.items`        | `item`: current state of a changed item         |
| `PROFILE` | `updates.profiles`     | `user`: username whose profile changed          |
| `TOP`     | `topstories` changed   | `items`: the current top 30 stories, in order   |

`Item` mirrors the [HN item](https://github.com/HackerNews/API#items) fields, without `kids`.

### Connecting other services

- **Kafka:** SASL/PLAIN over TLS. The username must be the service account the API key belongs to, and the password is `token:<JWT>`.
- **Schema Registry:** basic auth with the bare JWT as the password. The username is ignored.

For example, Kafka Connect's converter settings:

```properties
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=<SCHEMA_REGISTRY_URL>
value.converter.basic.auth.credentials.source=USER_INFO
value.converter.basic.auth.user.info=<service account>:<JWT>
```

Downstream jobs can route on `kind`. To change the schema, edit `hn_event.avsc` and restart the
producer. It registers the new version on start and refuses to run if the change isn't backward compatible.

## Dashboard

- **Front page / Rising**: switch between the current front page and observed stories with the largest point gains. Eight stories appear initially; expand the list to see the rest. Rank changes and point gains use up to 30 minutes of available history.
- **Radar**: the front-page stories, with #1 nearest the center. Each story has a stable angle; color shifts from sage to orange as it gains points. A scan completes one turn every six seconds, rank changes glide between rings, and fresh vote/comment updates emit a short pulse. Hover for details or click to open the discussion. Squares on the outer band are new submissions received in the last 10 minutes.
- **Live feed**: recent stories and comments, with their relative event age and author; hover the age for the full timestamp.
- **Live activity**: a continuously advancing 60-second view of observed events, grouped by the producer's timestamp into one-second bins. Empty seconds stay at zero; partial edge seconds and duplicate replays do not inflate the count. The clock and live-feed relative ages advance each second even between batches. Hover a feed timestamp to see the full time.
- **Activity**: events per minute over the last hour, split by type, with hover details. The current minute is partial. Front-page snapshots are excluded from event counts; other new item types, such as jobs and polls, are shown as Other when present.
- **Conversation leaders**: observed stories with the largest increase in total comment count, using up to 30 minutes of history.
- **Front-page sources**: domain counts among the current front-page stories. Text-only posts are grouped as Hacker News; remaining domains are included in the remainder count.
- **Topics / Emerging**: technical subjects and emerging comment topics from agent results, counted once per observed story when the stream supplies `story_id` with `subjects` or `emerging_topics`. These panels show a waiting state until analysis is supplied; the dashboard does not generate topic labels itself.
- **Latest submissions**: new stories received in the last hour, with All / Show HN / Ask HN filters and progressive expansion. Scores and comment counts follow later item updates.

The page scrolls naturally, with discussion, source, and submission views below the overview. These are
observations from this stream, not a census of all Hacker News activity. A story seen only once has no
measured growth. Shorter histories are not extrapolated to a full 30 minutes.

The header's theme selector supports System (the default), Light, and Dark. A manual choice is saved
in this browser and restored before first paint; System follows appearance changes while the page
is open. Chart colors, radar scanning, tooltips, and ranking/number animations adapt together.

Live changes roll the old number out and the new number in. Ranking changes move entire rows
between their old and new positions over roughly one second, with a brief lift and direction tint.
Consecutive ranking updates continue from the current visual position; value-only updates do not
restart row movement. Charts interpolate to their new values.
Existing rows retain their DOM nodes and keyboard focus; reading farther back in the live feed
keeps the visible scroll anchor. Motion respects the system's reduced-motion preference, and the
continuous radar loop pauses when it is outside the viewport or the page is hidden.
The connected status gently breathes, and radar points briefly glow as the sweep passes. Newly
received rows enter in a short stagger (at most 850ms in total), without delaying data ingestion.
The second-level activity view shares the same visibility-aware animation loop. These visual
indicators do not generate events, change story scores, or increase database polling frequency.

A new tab rebuilds all of this from the last hour of the view, then checks for new rows every
2 seconds. RisingWave builds the JSON for each row. Each SSE event id is the event's timestamp, so
when the browser reconnects it picks up where it left off.

## Troubleshooting

- **`<NAME> is not set (see .env.example)`:** fill in that setting in `.env`, then start the service again.
- **The producer logs connection failures, or `delivery to hn-events failed`:** StreamNative is rejecting the login. On a Kafka cluster, `KAFKA_USERNAME` must be exactly the service account the API key belongs to.
- **The dashboard shows no data:** `docker compose logs dashboard` shows the RisingWave error. Check `RISINGWAVE_URL`, and that `RISINGWAVE_MV` matches the view's name.

## Files

| File                 | What it is                                                         |
|----------------------|--------------------------------------------------------------------|
| `producer.py`        | Follows the HN streams and publishes Avro to Kafka                 |
| `dashboard.py`       | Serves the page, and streams events from RisingWave on `/events`   |
| `index.html`         | The dashboard page                                                 |
| `dashboard.css`      | Responsive layout and light/dark themes                             |
| `dashboard.js`       | UI rendering, controls, canvas charts, and SSE connection            |
| `radar-model.mjs`    | Rolling event state, rankings, growth, and source statistics         |
| `hn_event.avsc`      | Avro schema of the topic                                           |
| `Dockerfile`         | One image for both parts; runs the producer by default            |
| `docker-compose.yml` | Runs either part with Docker                                       |
| `.env.example`       | Settings template                                                  |

Data-model regression checks require Node.js 18+ and no extra packages: `node --test tests/radar-model.test.mjs`.

## Known limits

- If the producer restarts, items created while it was down are not backfilled.
- Every tab re-queries the view every 2 seconds, and without an index each query scans the whole view,
  which keeps every event. Add `CREATE INDEX ON "hn-events_mv"(ts)` once it gets large.
