"""Serve the radar page and stream the Kafka topic to browsers over Server-Sent Events.

Each browser connection gets its own Kafka consumer: a fresh tab replays the last hour of
the topic (so all panels are warm immediately), then follows it live. Records are Avro,
decoded via Schema Registry and forwarded as JSON. The SSE event id is the Kafka offset, so
when EventSource reconnects it resumes exactly where it left off.
"""
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

KAFKA = os.environ.get("KAFKA", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "hn-events")
PORT = int(os.environ.get("PORT", 8080))
REPLAY_MS = 60 * 60 * 1000
PAGE = Path(__file__).with_name("index.html")
deserialize = AvroDeserializer(SchemaRegistryClient({"url": os.environ.get("SCHEMA_REGISTRY", "http://localhost:8081")}))
context = SerializationContext(TOPIC, MessageField.VALUE)


def sse(msg):
    event = deserialize(msg.value(), context)
    # timestamp-millis fields decode to datetimes; the browser wants epoch millis
    return f"id: {msg.offset()}\ndata: {json.dumps(event, default=lambda d: round(d.timestamp() * 1000))}\n\n"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/events":
            body = PAGE.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        # ponytail: assumes the single-partition topic Kafka auto-creates, so an offset is a full position;
        # with more partitions the SSE id would need to carry one offset per partition
        consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "hn-radar-dashboard", "enable.auto.commit": False})
        try:
            last_id = self.headers.get("Last-Event-ID")  # sent by EventSource on reconnect
            if last_id:
                start = int(last_id) + 1
            else:  # offset -1 (= end of topic) when nothing is that recent
                since = TopicPartition(TOPIC, 0, int(time.time() * 1000) - REPLAY_MS)
                start = consumer.offsets_for_times([since], timeout=10)[0].offset
            consumer.assign([TopicPartition(TOPIC, 0, start)])
            while True:
                chunk = ""
                for msg in consumer.consume(500, timeout=15):
                    if msg.error():
                        continue
                    try:
                        chunk += sse(msg)
                    except SerializationError as e:  # e.g. a stray non-Avro test message: skip, don't wedge every tab
                        print(f"skipping offset {msg.offset()}: {e}")
                self.wfile.write((chunk or ": keep-alive\n\n").encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass  # browser tab went away
        finally:
            consumer.close()


print(f"HN radar on http://localhost:{PORT}  (kafka {KAFKA}, topic {TOPIC!r})")
ThreadingHTTPServer(("", PORT), Handler).serve_forever()
