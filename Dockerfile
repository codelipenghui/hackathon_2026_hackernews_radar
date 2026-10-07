FROM python:3.13-slim
RUN pip install --no-cache-dir "confluent-kafka[avro,schemaregistry]==2.16.0"
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY config.py producer.py dashboard.py index.html hn_event.avsc ./
