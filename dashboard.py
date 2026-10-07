"""Serve the cockpit and radar pages and stream Kafka topics to browsers over Server-Sent Events.

GET  /events?topics=a,b&run=R   one Kafka consumer per tab over partition 0 of each topic, starting at the run's
                                start (live: the last hour). Messages of other runs are skipped; hn-events always
                                passes, it is the live backdrop. The SSE id carries the offsets, so EventSource
                                reconnects resume exactly; with one topic it is the bare offset, as the radar expects.
POST /replay?file=F&speed=N     plays data/replays/F into Kafka as a new run and returns {"run": ...}
GET  /history?ticker=T&range=5d|1mo  older bars for the chart's 5D (15-minute) and 1M (daily) views, from yfinance
POST /research?ticker=T&depth=quick|deep   "Quick check" / "Deep research": publishes T's latest earnings 8-K (from SEC) to `filings` as a live,
                                on-demand filing, so the router starts ai-berkshire on it like any other filing
"""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from kafka import KafkaConsumer, TopicPartition

import replay
from common import HOUR_MS, KAFKA, LIVE, emit, ensure_topics, load_watchlist, new_replay_run, now_ms, producer, run_start_ms

PORT = int(os.environ.get("PORT", 8080))
HERE = Path(__file__).parent
REPLAYS = HERE / "data" / "replays"
PAGES = {"/": ("cockpit.html", "text/html; charset=utf-8"),
         "/radar": ("index.html", "text/html; charset=utf-8"),
         "/watchlist.json": ("watchlist.json", "application/json")}
RUNS = [LIVE]       # live, then replays started from this dashboard, newest first
LIVE_LOOKBACK_MS = 24 * HOUR_MS  # live prices and research keep the day; hn-events stays at the last hour (volume)
PRODUCER = None     # created in main(); only POST /replay writes to Kafka


def parse_last_id(value, topics):
    if not value:
        return {}
    if value.startswith("{"):
        return {t: int(o) for t, o in json.loads(value).items() if t in topics}
    return {topics[0]: int(value)}


def format_id(offsets, topics):
    return str(offsets[topics[0]]) if len(topics) == 1 else json.dumps(offsets, separators=(",", ":"))


def stream_start_ms(run, topic, now=None):
    if run == LIVE and topic != "hn-events":
        return (now_ms() if now is None else now) - LIVE_LOOKBACK_MS
    return run_start_ms(run, now)


def latest_earnings_filing(sub, ticker, depth="quick"):
    """The newest 8-K Item 2.02 in SEC submissions, as a live on-demand filings message (None if there is none)."""
    from record import filing_from_submissions, list_earnings

    found = list_earnings(sub, since_ms=0)
    if not found:
        return None
    accession, _ = max(found, key=lambda x: x[1])
    return {**filing_from_submissions(sub, ticker, accession), "run": LIVE, "on_demand": True, "depth": depth}


class History:
    """Chart history from yfinance, cached briefly: it isn't a stream, so it bypasses Kafka."""
    RANGES = {"5d": "15m", "1mo": "1d"}
    TTL_S = 60

    def __init__(self, fetch, clock=None):
        import time
        self.fetch, self.clock, self.cache = fetch, clock or time.time, {}

    def get(self, ticker, period):
        if period not in self.RANGES:
            raise ValueError(f"range must be one of {sorted(self.RANGES)}")
        now, hit = self.clock(), self.cache.get((ticker, period))
        if hit and now - hit[0] < self.TTL_S:
            return hit[1]
        from record import ticks_from_bars
        ticks = ticks_from_bars(self.fetch(ticker, period, self.RANGES[period]), ticker)
        self.cache[(ticker, period)] = (now, ticks)
        return ticks


def fetch_history(ticker, period, interval):
    import yfinance as yf

    df = yf.Ticker(ticker).history(period=period, interval=interval, prepost=False)
    return [(when.to_pydatetime(), row.Close, row.Volume) for when, row in df.iterrows()]


HISTORY = History(fetch_history)


def replay_path(name):
    path = REPLAYS / name
    return path if name and path.parent == REPLAYS and path.is_file() else None


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlsplit(self.path)
        q = parse_qs(url.query)
        if url.path == "/events":
            return self.stream(q.get("topics", ["hn-events"])[0].split(","), q.get("run", [None])[0])
        if url.path == "/replays":
            return self.send_json(sorted(p.name for p in REPLAYS.glob("*.jsonl")))
        if url.path == "/runs":
            return self.send_json(RUNS)
        if url.path == "/history":
            ticker = q.get("ticker", [""])[0]
            if ticker not in load_watchlist():
                return self.send_error(400, "ticker not on the watchlist")
            try:
                return self.send_json(HISTORY.get(ticker, q.get("range", [""])[0]))
            except ValueError as e:
                return self.send_error(400, str(e))
            except Exception as e:
                return self.send_error(502, f"history lookup failed: {e!r}"[:200])
        if url.path not in PAGES:
            return self.send_error(404)
        name, ctype = PAGES[url.path]
        body = (HERE / name).read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        url = urlsplit(self.path)
        q = parse_qs(url.query)
        if url.path == "/research":
            return self.research_now(q.get("ticker", [""])[0], q.get("depth", ["quick"])[0])
        path = replay_path(q.get("file", [""])[0])
        if url.path != "/replay" or not path:
            return self.send_error(400, "unknown replay file")
        run = new_replay_run()
        RUNS.insert(1, run)
        threading.Thread(target=replay.play, args=(path, float(q.get("speed", ["300"])[0]), PRODUCER, run),
                         daemon=True).start()
        self.send_json({"run": run})

    def research_now(self, ticker, depth):
        from record import submissions

        watchlist = load_watchlist()
        if ticker not in watchlist:
            return self.send_error(400, "ticker not on the watchlist")
        if depth not in ("quick", "deep"):
            return self.send_error(400, "depth must be quick or deep")
        try:
            filing = latest_earnings_filing(submissions(watchlist[ticker]["cik"]), ticker, depth)
        except Exception as e:
            return self.send_error(502, f"SEC lookup failed: {e!r}"[:200])
        if not filing:
            return self.send_error(404, "no earnings 8-K found")
        emit(PRODUCER, "filings", ticker, filing)
        PRODUCER.flush()
        self.send_json({"ticker": ticker, "accession": filing["accession"], "ts": filing["ts"], "depth": depth})

    def send_json(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def stream(self, topics, run):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        ensure_topics(topics)
        tps = [TopicPartition(t, 0) for t in topics]
        consumer = KafkaConsumer(bootstrap_servers=KAFKA)
        try:
            consumer.assign(tps)
            offsets = parse_last_id(self.headers.get("Last-Event-ID"), topics)  # sent by EventSource on reconnect
            starts = consumer.offsets_for_times({tp: stream_start_ms(run or LIVE, tp.topic) for tp in tps})
            for tp in tps:
                if tp.topic in offsets:
                    consumer.seek(tp, offsets[tp.topic] + 1)
                elif starts[tp]:
                    consumer.seek(tp, starts[tp].offset)
                else:
                    consumer.seek_to_end(tp)
            while True:
                chunk = []
                for tp, records in consumer.poll(timeout_ms=15000).items():
                    for r in records:
                        offsets[tp.topic] = r.offset
                        msg = json.loads(r.value)
                        if run and tp.topic != "hn-events" and msg.get("run", LIVE) != run:
                            continue
                        msg["topic"] = tp.topic
                        chunk.append(f"id: {format_id(offsets, topics)}\ndata: {json.dumps(msg)}\n\n")
                self.wfile.write(("".join(chunk) or ": keep-alive\n\n").encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass  # browser tab went away
        finally:
            consumer.close()


def main():
    global PRODUCER
    PRODUCER = producer()
    print(f"Alpha Radar on http://localhost:{PORT}  (radar at /radar, kafka {KAFKA})")
    ThreadingHTTPServer(("", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
