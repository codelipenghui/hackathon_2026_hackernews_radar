FROM python:3.13-slim
RUN pip install --no-cache-dir kafka-python==3.0.11
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY producer.py dashboard.py index.html ./
