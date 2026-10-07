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

## Publish to StreamNative Cloud

The producer and the dashboard can use a StreamNative Cloud cluster, through its Kafka protocol
and Kafka Schema Registry, instead of the local containers:

```sh
cp .env.example .env    # fill it in; .env is git-ignored
docker compose up -d --build
```

- **Kafka cluster:** set `KAFKA_USERNAME` to the service account the API key belongs to, the `userName` in the console's client example. The broker rejects any other username.
- **Pulsar cluster:** leave `KAFKA_USERNAME` empty, so it defaults to `public/default`, and append `/kafka` to the Schema Registry URL.
- The service account needs produce and consume permission on the topic, plus the `schema-writer` role.
- On start the producer registers the schema and creates `hn-events` with one partition. If the account can't create topics, create `hn-events` yourself with one partition.
- On a Pulsar cluster, give `public/default` a retention policy, e.g. 7 days. Otherwise Pulsar can drop messages that no subscription holds, and the dashboard's one-hour replay comes back empty.
- Other services connect the same way. Kafka uses SASL/PLAIN over TLS with the username above and password `token:<JWT>`. The registry uses basic auth with the bare JWT as password. See [`config.py`](config.py).

To go back to local, move `.env` aside and run `docker compose up -d` again. The local Kafka and
Schema Registry containers keep running in either mode.

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

- The dashboard reads partition 0 only. The producer creates the topic with one partition, and warns if an existing topic has more.
- If the producer restarts, items created while it was down are not backfilled.
