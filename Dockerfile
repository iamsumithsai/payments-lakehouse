FROM python:3.11-slim-bookworm

# Spark needs Java. procps is needed by Spark's launch scripts.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

# The project folder is mounted at /app by docker-compose, so code changes need no rebuild.
CMD ["python", "src/bronze.py"]
