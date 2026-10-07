"""Shared plumbing for every Alpha Radar service: Kafka helpers, run ids, the watchlist, message contracts.

Every message on every topic carries `run` ("live", or "replay-<unix seconds>" for a recorded earnings day played
back into Kafka) and `ts` (event time, epoch ms). Services key their state by run and reason only about ts, so a
replay at 300x behaves exactly like the day it recorded, and never mixes with live data.
"""
import json
import os
import re
import time
from pathlib import Path

KAFKA = os.environ.get("KAFKA", "localhost:9092")
LIVE = "live"
HOUR_MS = 60 * 60 * 1000
WATCHLIST = Path(__file__).with_name("watchlist.json")

# required keys per topic (spec §3); tests check fixtures and service output against these
REQUIRED = {
    "filings": {"run", "ts", "ticker", "cik", "form", "items", "accession", "url", "title"},
    "ticks": {"run", "ts", "ticker", "price", "volume", "session"},
    "news": {"run", "ts", "ticker", "source", "headline", "url"},
    "skill-requests": {"run", "ts", "request_id", "ticker", "skill", "args", "reason", "trigger"},
    "research-verdicts": {"run", "ts", "request_id", "ticker", "status", "skill", "tier"},
    "orders": {"run", "ts", "order_id", "ticker", "side", "qty", "rule", "reason", "request_id"},
    "fills": {"run", "ts", "order_id", "ticker", "side", "qty", "price"},
    "pnl": {"run", "ts", "cash", "equity", "realized", "unrealized", "positions"},
    "news-triage": {"run", "ts", "ticker", "headline", "url", "source", "material", "direction", "reason", "request_id"},
}
VERDICT_DONE = {"source", "verdict", "score", "masters", "buy_low", "buy_high", "target",
                "red_lines", "summary", "report_path", "duration_s"}


def missing_keys(topic, msg):
    need = set(REQUIRED[topic])
    if topic == "research-verdicts" and msg.get("status") == "done":
        need |= VERDICT_DONE
    return need - msg.keys()


def now_ms():
    return int(time.time() * 1000)


_last_run = ["", 0]  # (base id, how many runs started in that second)


def new_replay_run():
    """replay-<unix seconds>; a second run in the same second (a double-click) gets -2, -3, ..."""
    base = f"replay-{int(time.time())}"
    _last_run[:] = [base, _last_run[1] + 1 if _last_run[0] == base else 1]
    return base if _last_run[1] == 1 else f"{base}-{_last_run[1]}"


def run_start_ms(run, now=None):
    """Where a run begins in Kafka (record timestamps are produce time): a replay starts at the second in its id,
    live shows the last hour."""
    if run.startswith("replay-"):
        return int(run.split("-")[1]) * 1000
    return (now_ms() if now is None else now) - HOUR_MS


def load_watchlist(path=WATCHLIST):
    return json.loads(Path(path).read_text())


def match_tickers(text, watchlist):
    """Watchlist tickers whose company name or alias appears in text as a whole word. Case-sensitive, so
    "Micron" matches but "5 micron process" does not."""
    return [ticker for ticker, info in watchlist.items()
            if any(re.search(rf"\b{re.escape(n)}\b", text) for n in info["names"] + info.get("aliases", []))]


def ensure_topics(topics):
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=KAFKA)
    try:
        new = [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics if t not in set(admin.list_topics())]
        if new:
            try:
                admin.create_topics(new)
            except TopicAlreadyExistsError:
                pass  # another service created it first
    finally:
        admin.close()


def producer():
    from kafka import KafkaProducer

    return KafkaProducer(bootstrap_servers=KAFKA, key_serializer=str.encode,
                         value_serializer=lambda v: json.dumps(v).encode(), linger_ms=20)


def emit(prod, topic, key, msg):
    prod.send(topic, key=str(key), value=msg)


def consume(topics, start_ms=None):
    """Yield (topic, msg) from partition 0 of each topic, starting at start_ms (produce time), or from now on if None."""
    from kafka import KafkaConsumer, TopicPartition

    ensure_topics(topics)
    consumer = KafkaConsumer(bootstrap_servers=KAFKA, value_deserializer=json.loads)
    tps = [TopicPartition(t, 0) for t in topics]
    consumer.assign(tps)
    if start_ms is None:
        consumer.seek_to_end(*tps)
    else:
        found = consumer.offsets_for_times({tp: start_ms for tp in tps})
        for tp in tps:
            consumer.seek(tp, found[tp].offset) if found[tp] else consumer.seek_to_end(tp)
    while True:
        for tp, records in consumer.poll(timeout_ms=1000).items():
            for r in records:
                yield tp.topic, r.value
