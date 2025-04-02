FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Number of days of 24h performance data to show with /operator command
ENV OPERATOR_PERFORMANCE_DAYS=7

# Number of outbound API calls per minute
ENV REQUESTS_PER_MINUTE=20

ENV CLICKHOUSE_HOST=clickhouse
ENV CLICKHOUSE_USER=ssv_performance
ENV CLICKHOUSE_PASSWORD=dsFtsxfSfNKd42uDrkp47gt22cfgG8sh

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "ssv-performance-collector.py"]