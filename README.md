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

## Run

```sh
docker compose up -d --build
open http://localhost:8080
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
