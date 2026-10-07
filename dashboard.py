"""Serve the radar page and stream Hacker News events to browsers over Server-Sent Events.

The events come from a RisingWave materialized view with the HnEvent columns, e.g. a mirror of a
RisingWave Kafka source on the topic. Each browser tab replays the last hour, then checks for new
rows every 2 seconds; RisingWave builds the JSON for each row. The SSE event id is the event's ts,
so when EventSource reconnects it resumes where it left off.
"""
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg
from psycopg import sql

RISINGWAVE_URL = os.environ["RISINGWAVE_URL"]
VIEW = os.environ.get("RISINGWAVE_MV") or "hn-events_mv"
PORT = int(os.environ.get("PORT", 8080))
REPLAY_MS = 60 * 60 * 1000
PAGE = Path(__file__).with_name("index.html")
QUERY = sql.SQL("""
    SELECT (extract(epoch FROM ts) * 1000)::bigint,
           jsonb_build_object('kind', kind, 'ts', (extract(epoch FROM ts) * 1000)::bigint, 'item', to_jsonb(item),
                              'user', "user", 'items', to_jsonb(items))::varchar
    FROM {} WHERE ts > to_timestamp(%s::double precision / 1000) ORDER BY ts""").format(sql.Identifier(VIEW))


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
        try:
            self.stream(self.headers.get("Last-Event-ID"))  # EventSource sends the last id when it reconnects
        except (BrokenPipeError, ConnectionResetError):
            pass  # browser tab went away

    def stream(self, last_id):
        self.send(f"event: source\ndata: RisingWave view {VIEW}\n\n")
        # ponytail: every tab re-queries the view every 2s, and with no index on ts each query scans the whole
        # view; add `CREATE INDEX ON "hn-events_mv"(ts)` (or one shared poller) when the view or audience grows
        newest = max(int(last_id or 0), int(time.time() * 1000) - REPLAY_MS)  # never replay more than an hour
        lower, sent = newest, {}  # sent: events inside the re-read window -> ts
        with psycopg.connect(RISINGWAVE_URL, autocommit=True, connect_timeout=15) as conn:
            while True:
                chunk = ""
                for ts, event in conn.execute(QUERY, (lower,)):
                    if event not in sent:
                        sent[event] = ts
                        newest = max(newest, ts)
                        chunk += f"id: {ts}\ndata: {event}\n\n"
                # rows commit in ~1s batches and can land slightly out of ts order: re-read the last 10s,
                # skipping what was already sent
                lower = newest - 10_000
                sent = {event: ts for event, ts in sent.items() if ts > lower}
                self.send(chunk)
                time.sleep(2)

    def send(self, chunk):
        self.wfile.write((chunk or ": keep-alive\n\n").encode())
        self.wfile.flush()


print(f"HN radar on http://localhost:{PORT}  (RisingWave view {VIEW})")
ThreadingHTTPServer(("", PORT), Handler).serve_forever()
