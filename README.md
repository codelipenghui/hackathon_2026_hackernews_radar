# HN Radar

A real-time Hacker News dashboard. The HN API is hosted on Firebase, and Firebase supports
**streaming**: send `Accept: text/event-stream` to any endpoint and you get Server-Sent Events
whenever that node changes. HN updates the nodes in batches about every 30s.

```
hacker-news.firebaseio.com        producer.py                   Kafka (KRaft, 1 node)         dashboard.py          browser
  /v0/maxitem    (SSE) ─┐                                                                                         
  /v0/updates    (SSE) ─┼─▶ fetch items ─▶ Avro ─▶  topic hn-events (7d retention) ─▶ consumer per tab ─SSE─▶ index.html
  /v0/topstories (SSE) ─┘                   │                                            │  (replays last 1h, then live)
                                            └────▶ Schema Registry (hn-events-value) ◀───┘
```

## Run

```sh
docker compose up -d --build
open http://localhost:8080
```

| Service         | From your machine        | From other containers         |
|-----------------|--------------------------|-------------------------------|
| Dashboard       | `http://localhost:8080`  | `http://dashboard:8080`       |
| Kafka           | `localhost:9092`         | `kafka:19092`                 |
| Schema Registry | `http://localhost:8081`  | `http://schema-registry:8081` |

## Topic `hn-events`

- **Key:** a plain UTF-8 string: the item id, the username, or `top`.
- **Value:** Avro in the Confluent wire format. The schema is [`hn_event.avsc`](hn_event.avsc), registered as subject `hn-events-value` with the registry's default BACKWARD compatibility.

Every record is an `HnEvent` with `kind`, `ts` (`timestamp-millis`), and one field that depends on the kind:

| kind      | source                 | field set                                       |
|-----------|------------------------|-------------------------------------------------|
| `NEW`     | `maxitem` moved        | `item`: new story / comment / job / poll        |
| `UPDATE`  | `updates.items`        | `item`: current state of a changed item         |
| `PROFILE` | `updates.profiles`     | `user`: username whose profile changed          |
| `TOP`     | `topstories` changed   | `items`: the current top 30 stories, in order   |

`Item` mirrors the [HN item](https://github.com/HackerNews/API#items) fields, without `kids`.

```sh
curl localhost:8081/subjects/hn-events-value/versions/latest   # the registered schema
docker compose exec schema-registry kafka-avro-console-consumer --bootstrap-server kafka:19092 \
  --topic hn-events --from-beginning --max-messages 5 --property schema.registry.url=http://localhost:8081
```

### Connecting other services

Use the Confluent Avro deserializer and point it at the registry. Clients in other languages work the same way through their Confluent Avro deserializer. For Kafka Connect:

```properties
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=io.confluent.connect.avro.AvroConverter
value.converter.schema.registry.url=http://schema-registry:8081
```

Downstream jobs can route on `kind`. To change the schema, edit `hn_event.avsc` and restart the
producer. It registers the new version on start and refuses to run if the change isn't backward compatible.

## Dashboard

- **Radar**: the 30 front-page stories. Distance from the center is the rank (#1 in the middle). The angle is fixed per story, so you can watch stories move in or out. Color goes from green to orange as the story gains points over 30 minutes. A ring pulses when a story gets new votes or comments. The outer band shows new submissions from the last 10 minutes.
- **Front page**: the current top 30 with rank change over the last 30 minutes, plus points gained.
- **Rising**: the stories that gained the most points in the last 30 minutes.
- **Activity**: events per minute over the last hour, split by type.
- **Live feed**: new stories and comments.

A new tab rebuilds all of this from the last hour of the topic. The dashboard decodes the Avro
via the registry and sends JSON to the browser. Each SSE event id is the Kafka offset, so when the
browser reconnects it picks up exactly where it left off.

## Known limits

- The dashboard assumes `hn-events` has a single partition, which is what Kafka auto-creates.
- If the producer restarts, items created while it was down are not backfilled.
