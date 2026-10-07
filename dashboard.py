"""Serve the radar page and stream the Kafka topic to browsers over Server-Sent Events.

Each browser connection gets its own Kafka consumer: a fresh tab replays the last hour of
the topic (so all panels are warm immediately), then follows it live. The SSE event id is the
Kafka offset, so when EventSource reconnects it resumes exactly where it left off.
"""
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition

KAFKA = os.environ.get("KAFKA", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "hn-events")
PORT = int(os.environ.get("PORT", 8080))
REPLAY_MS = 60 * 60 * 1000
PAGE = Path(__file__).with_name("index.html")


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
        tp = TopicPartition(TOPIC, 0)
        consumer = KafkaConsumer(bootstrap_servers=KAFKA)
        try:
            consumer.assign([tp])
            last_id = self.headers.get("Last-Event-ID")  # sent by EventSource on reconnect
            if last_id:
                consumer.seek(tp, int(last_id) + 1)
            else:
                start = consumer.offsets_for_times({tp: int(time.time() * 1000) - REPLAY_MS})[tp]
                consumer.seek(tp, start.offset) if start else consumer.seek_to_end(tp)
            while True:
                records = consumer.poll(timeout_ms=15000).get(tp, [])
                chunk = "".join(f"id: {r.offset}\ndata: {r.value.decode()}\n\n" for r in records)
                self.wfile.write((chunk or ": keep-alive\n\n").encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass  # browser tab went away
        finally:
            consumer.close()


print(f"HN radar on http://localhost:{PORT}  (kafka {KAFKA}, topic {TOPIC!r})")
ThreadingHTTPServer(("", PORT), Handler).serve_forever()
