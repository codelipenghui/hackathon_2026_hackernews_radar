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

## Run

You need:

1. **StreamNative Cloud:** a Kafka cluster with the topic `hn-events`. A service account needs produce permission on the topic, plus the `schema-writer` role. The producer registers the schema on start.
2. **RisingWave:** a Kafka source on `hn-events` that decodes the Avro through the schema registry. The dashboard reads this view of it:
   ```sql
   CREATE MATERIALIZED VIEW "hn-events_mv" AS SELECT * FROM "hn-events";  -- "hn-events": the Kafka source
   ```

Then:

```sh
cp .env.example .env    # fill it in; .env is git-ignored
docker compose up -d --build
open http://localhost:8080
```

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

## Known limits

- If the producer restarts, items created while it was down are not backfilled.
- Every tab re-queries the view every 2 seconds, and without an index each query scans the whole view,
  which keeps every event. Add `CREATE INDEX ON "hn-events_mv"(ts)` once it gets large.
