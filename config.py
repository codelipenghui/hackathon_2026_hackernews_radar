"""Connection settings shared by producer.py and dashboard.py.

Defaults to the local Kafka + Schema Registry. Setting JWT_TOKEN (see .env.example) switches both
to StreamNative Cloud: SASL/PLAIN over TLS for Kafka, basic auth for the Schema Registry.
"""
import os

KAFKA = os.environ.get("KAFKA_SERVICE_URL") or "localhost:9092"
TOPIC = os.environ.get("TOPIC") or "hn-events"
TOKEN = os.environ.get("JWT_TOKEN", "").strip()
# Kafka clusters only accept the service account the token belongs to; Pulsar clusters take tenant/namespace
USERNAME = os.environ.get("KAFKA_USERNAME", "").strip() or "public/default"

kafka_conf = {"bootstrap.servers": KAFKA}
registry_conf = {"url": os.environ.get("SCHEMA_REGISTRY_URL") or "http://localhost:8081"}
if TOKEN:
    # Kafka takes "token:<jwt>", the registry the bare jwt (and ignores the username)
    kafka_conf |= {"security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN",
                   "sasl.username": USERNAME, "sasl.password": f"token:{TOKEN}"}
    registry_conf["basic.auth.user.info"] = f"{USERNAME}:{TOKEN}"
