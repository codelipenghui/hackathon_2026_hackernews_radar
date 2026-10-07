FROM python:3.13-slim
RUN pip install --no-cache-dir "confluent-kafka[avro,schemaregistry]==2.16.0" "psycopg[binary]==3.3.6"
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY producer.py dashboard.py index.html hn_event.avsc ./
