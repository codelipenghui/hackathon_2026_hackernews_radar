"""Play a recorded earnings day (data/replays/*.jsonl) into Kafka as a new run, N times faster than it happened.

  python replay.py data/replays/MU-2025-09-23.jsonl [--speed 300]

Messages keep their historical ts; only the pacing is compressed, and any single gap (overnight, weekend) is capped
at MAX_GAP_S so the demo never stalls. The last tick is marked "last" so the trader closes out and the scorer shows
realized P&L.
"""
import argparse
import json
import time
from pathlib import Path

from common import emit, new_replay_run, producer

MAX_GAP_S = 5


def load(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def schedule(lines, speed, run):
    last_tick = max((i for i, line in enumerate(lines) if line["topic"] == "ticks"), default=None)
    out, prev = [], None
    for i, line in enumerate(lines):
        msg = {**line["msg"], "run": run}
        if i == last_tick:
            msg["last"] = True
        gap = 0 if prev is None else min(MAX_GAP_S, max(0, (msg["ts"] - prev) / 1000 / speed))
        prev = msg["ts"]
        out.append((gap, line["topic"], msg))
    return out


def play(path, speed, prod, run=None):
    run = run or new_replay_run()
    for gap, topic, msg in schedule(load(path), speed, run):
        time.sleep(gap)
        emit(prod, topic, msg.get("ticker", "_"), msg)
    prod.flush()
    return run


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--speed", type=float, default=300)
    a = ap.parse_args()
    run = new_replay_run()
    print(f"replaying {a.file} as {run} at {a.speed:g}x")
    play(a.file, a.speed, producer(), run)
    print("done")
