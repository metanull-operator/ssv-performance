FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV CLICKHOUSE_PASSWORD=

# Number of outbound API calls per minute
#ENV REQUESTS_PER_MINUTE=20

# Log level for the collector
# Possible values: DEBUG, INFO, WARNING, ERROR, CRITICAL
#ENV COLLECTOR_LOG_LEVEL=INFO

# Number of days of 24h performance data to show with /operator command
#ENV OPERATOR_PERFORMANCE_DAYS=7

# Interoperability and logging
ENV CLICKHOUSE_HOST=clickhouse
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "ssv-performance-collector.py"]