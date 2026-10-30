FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY producer /app/producer
COPY storage /app/storage
COPY spark /app/spark
ENV PYTHONUNBUFFERED=1
