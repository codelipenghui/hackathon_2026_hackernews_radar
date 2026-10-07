"""Follow the Hacker News Firebase API in real time and publish every change to Kafka.

Firebase REST streaming (Server-Sent Events) pushes a new value for a node whenever it changes;
HN publishes maxitem / updates / topstories in ~30s ticks. Each tick becomes Kafka messages:

  {"kind": "new",     "ts": ms, "item": {...}}    a newly created story/comment/job/poll
  {"kind": "update",  "ts": ms, "item": {...}}    an item that changed (score, comments, edits)
  {"kind": "profile", "ts": ms, "id": "user"}     a user profile that changed
  {"kind": "top",     "ts": ms, "items": [...]}   the current front page (top 30)
"""
import json
import os
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from kafka import KafkaProducer

API = "https://hacker-news.firebaseio.com/v0"
TOPIC = os.environ.get("TOPIC", "hn-events")
producer = KafkaProducer(
    bootstrap_servers=os.environ.get("KAFKA", "localhost:9092"),
    key_serializer=str.encode,
    value_serializer=lambda v: json.dumps(v).encode(),
    compression_type="gzip",
    linger_ms=100,
)
pool = ThreadPoolExecutor(16)


def get(path):
    with urllib.request.urlopen(f"{API}/{path}.json", timeout=20) as resp:
        return json.load(resp)


def fetch_item(item_id):
    try:
        item = get(f"item/{item_id}")
    except Exception as e:
        print(f"item {item_id}: {e!r}")
        return None
    if item:
        item.pop("kids", None)  # child-id lists bloat every message; `parent` links already encode the tree
    return item


def emit(kind, key, **data):
    producer.send(TOPIC, key=str(key), value={"kind": kind, "run": "live", "ts": int(time.time() * 1000), **data})


def follow(path, on_change):
    """Stream a Firebase node and call on_change(value) with its full value on every change."""
    while True:
        try:
            req = urllib.request.Request(f"{API}/{path}.json", headers={"Accept": "text/event-stream"})
            with urllib.request.urlopen(req, timeout=90) as resp:  # Firebase sends keep-alives every 30s
                event = None
                for line in resp:
                    line = line.decode().strip()
                    if line.startswith("event:"):
                        event = line[6:].strip()
                        if event in ("cancel", "auth_revoked"):
                            break
                    elif line.startswith("data:") and event in ("put", "patch"):
                        msg = json.loads(line[5:])
                        # HN always puts the whole value at the root; re-read the node for anything partial
                        on_change(msg["data"] if event == "put" and msg["path"] == "/" else get(path))
        except Exception as e:
            print(f"{path} stream: {e!r}, reconnecting")
        time.sleep(3)


max_seen = None
last_updates = None


def on_maxitem(max_id):
    global max_seen
    if max_seen is not None:  # first value is just the baseline (also bridges stream reconnects)
        ids = pending = list(range(max_seen + 1, max_id + 1))
        for _ in range(5):  # new items only become readable ~2-3s after maxitem moves
            time.sleep(2)
            items = list(pool.map(fetch_item, pending))
            for item in filter(None, items):
                emit("new", item["id"], item=item)
            pending = [i for i, item in zip(pending, items) if not item]
            if not pending:
                break
        print(f"new: {len(ids) - len(pending)}/{len(ids)} items")
    max_seen = max(max_id, max_seen or 0)


def on_updates(updates):
    global last_updates
    if updates == last_updates:  # a reconnect replays the current value
        return
    last_updates = updates
    items = [i for i in pool.map(fetch_item, updates.get("items", [])) if i]
    for item in items:
        emit("update", item["id"], item=item)
    for name in updates.get("profiles", []):
        emit("profile", name, id=name)
    print(f"updates: {len(items)} items, {len(updates.get('profiles', []))} profiles")


def on_topstories(ids):
    items = [i for i in pool.map(fetch_item, ids[:30]) if i]
    emit("top", "top", items=items)
    print(f"top: {len(items)} stories")


for path, handler in [("maxitem", on_maxitem), ("updates", on_updates), ("topstories", on_topstories)]:
    threading.Thread(target=follow, args=(path, handler), daemon=True).start()
print(f"streaming Hacker News -> kafka topic {TOPIC!r}")
threading.Event().wait()
