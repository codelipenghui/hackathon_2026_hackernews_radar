"""Follow the Hacker News Firebase API in real time and publish every change to Kafka on StreamNative Cloud.

Firebase REST streaming (Server-Sent Events) pushes a new value for a node whenever it changes;
HN publishes maxitem / updates / topstories in ~30s ticks. Each change becomes one Avro HnEvent
(hn_event.avsc, registered in Schema Registry as subject "<topic>-value"), keyed by item id/username:

  NEW      item    a newly created story/comment/job/poll
  UPDATE   item    an item that changed (score, comments, edits)
  PROFILE  user    a user profile that changed
  TOP      items   the current front page (top 30)
"""
import json
import os
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


def setting(name):
    return os.environ.get(name, "").strip() or sys.exit(f"{name} is not set (see .env.example)")


API = "https://hacker-news.firebaseio.com/v0"
KAFKA = setting("KAFKA_SERVICE_URL")
TOPIC = os.environ.get("TOPIC") or "hn-events"
SCHEMA = Path(__file__).with_name("hn_event.avsc").read_text()
TOKEN = setting("JWT_TOKEN")
USER = setting("KAFKA_USERNAME")  # the service account the token belongs to; the broker rejects any other

# Kafka takes SASL/PLAIN with "token:<jwt>", the registry basic auth with the bare jwt
registry = SchemaRegistryClient({"url": setting("SCHEMA_REGISTRY_URL"), "basic.auth.user.info": f"{USER}:{TOKEN}"})
schema_id = registry.register_schema(f"{TOPIC}-value", Schema(SCHEMA, "AVRO"))  # fails fast if incompatible
serialize = AvroSerializer(registry, SCHEMA)
context = SerializationContext(TOPIC, MessageField.VALUE)
producer = Producer({
    "bootstrap.servers": KAFKA,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": USER,
    "sasl.password": f"token:{TOKEN}",
    "enable.idempotence": True,
    "compression.type": "gzip",
    "linger.ms": 100,
    "delivery.report.only.error": True,  # e.g. a missing produce permission would otherwise fail silently
    "on_delivery": lambda err, msg: print(f"delivery to {msg.topic()} failed: {err.str()}"),
})
pool = ThreadPoolExecutor(16)


def get(path):
    with urllib.request.urlopen(f"{API}/{path}.json", timeout=20) as resp:
        return json.load(resp)


def fetch_item(item_id):
    try:
        return get(f"item/{item_id}")  # fields not in the schema (kids) are dropped on serialization
    except Exception as e:
        print(f"item {item_id}: {e!r}")
        return None


def emit(kind, key, **fields):
    try:
        value = serialize({"kind": kind, "ts": int(time.time() * 1000), **fields}, context)
    except Exception as e:  # one item that breaks the schema must not abort (and endlessly retry) its whole batch
        print(f"skipping {kind} {key}: {e!r}")
        return
    producer.produce(TOPIC, key=str(key), value=value)
    producer.poll(0)  # serve delivery reports


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
                emit("NEW", item["id"], item=item)
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
    # HN occasionally lists the same id twice in one batch; publish it once
    items = [i for i in pool.map(fetch_item, dict.fromkeys(updates.get("items", []))) if i]
    profiles = list(dict.fromkeys(updates.get("profiles", [])))
    for item in items:
        emit("UPDATE", item["id"], item=item)
    for name in profiles:
        emit("PROFILE", name, user=name)
    print(f"updates: {len(items)} items, {len(profiles)} profiles")


def on_topstories(ids):
    items = [i for i in pool.map(fetch_item, ids[:30]) if i]
    emit("TOP", "top", items=items)
    print(f"top: {len(items)} stories")


for path, handler in [("maxitem", on_maxitem), ("updates", on_updates), ("topstories", on_topstories)]:
    threading.Thread(target=follow, args=(path, handler), daemon=True).start()
print(f"streaming Hacker News -> {KAFKA} topic {TOPIC!r} (Avro, schema id {schema_id})")
threading.Event().wait()
