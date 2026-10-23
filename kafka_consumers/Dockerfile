FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY kafka_consumers/ ./kafka_consumers/
COPY dashboard/ ./dashboard/

ENV PYTHONPATH=/app