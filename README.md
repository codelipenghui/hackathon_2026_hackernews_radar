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

- **Radar**: the 30 front-page stories. Distance from the center is the rank (#1 in the middle). The angle is fixed per story, so you can watch stories move in or out. Color goes from green to orange as the story gains points over 30 minutes. A ring pulses when a story gets new votes or comments. The outer band shows new submissions from the last 10 minutes.
- **Front page**: the current top 30 with rank change over the last 30 minutes, plus points gained.
- **Rising**: the stories that gained the most points in the last 30 minutes.
- **Activity**: events per minute over the last hour, split by type.
- **Live feed**: new stories and comments.

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
| `hn_event.avsc`      | Avro schema of the topic                                           |
| `Dockerfile`         | One image for both parts; runs the producer by default            |
| `docker-compose.yml` | Runs either part with Docker                                       |
| `.env.example`       | Settings template                                                  |

## Known limits

- If the producer restarts, items created while it was down are not backfilled.
- Every tab re-queries the view every 2 seconds, and without an index each query scans the whole view,
  which keeps every event. Add `CREATE INDEX ON "hn-events_mv"(ts)` once it gets large.
