"""End-to-end check against the running stack: replay a recorded day and expect research, a buy, an exit, and a
flat book. Needs `docker compose up -d --build`, `python research.py` running, and a cached verdict for the file.

  python e2e.py data/replays/MU-2025-09-23.jsonl
"""
import json
import sys
import threading
import time
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition

import replay
from common import KAFKA, ensure_topics, new_replay_run, producer, run_start_ms

TOPICS = ["research-verdicts", "orders", "pnl"]


def main(path):
    ensure_topics(TOPICS)
    run = new_replay_run()
    player = threading.Thread(target=replay.play, args=(Path(path), 600, producer(), run))
    player.start()
    consumer = KafkaConsumer(bootstrap_servers=KAFKA, value_deserializer=json.loads)
    tps = [TopicPartition(t, 0) for t in TOPICS]
    consumer.assign(tps)
    starts = consumer.offsets_for_times({tp: run_start_ms(run) for tp in tps})
    for tp in tps:
        consumer.seek(tp, starts[tp].offset) if starts[tp] else consumer.seek_to_end(tp)
    seen, last_pnl, deadline = {"verdict": None, "orders": []}, None, time.time() + 240
    while time.time() < deadline:
        for tp, records in consumer.poll(timeout_ms=1000).items():
            for r in records:
                m = r.value
                if m.get("run") != run:
                    continue
                if tp.topic == "research-verdicts" and m["status"] != "started":
                    seen["verdict"] = m
                elif tp.topic == "orders":
                    seen["orders"].append(f"{m['side']} {m['qty']} {m['rule']}")
                elif tp.topic == "pnl":
                    last_pnl = m
        if not player.is_alive() and last_pnl and not last_pnl["positions"] and seen["orders"]:
            break
    print(f"run {run}\nverdict: {seen['verdict'] and seen['verdict'].get('verdict')} ({seen['verdict'] and seen['verdict'].get('source')})")
    print("orders:", seen["orders"])
    print("final pnl:", last_pnl)
    ok = seen["verdict"] and seen["verdict"]["status"] == "done" and len(seen["orders"]) >= 2 and last_pnl and not last_pnl["positions"]
    print("E2E PASS" if ok else "E2E FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main(sys.argv[1])
