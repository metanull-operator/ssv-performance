FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV CLICKHOUSE_PASSWORD=

# Log level for the migration script
# Possible values: DEBUG, INFO, WARNING, ERROR, CRITICAL
#ENV MIGRATION_LOG_LEVEL=INFO

# Interoperability and logging
ENV CLICKHOUSE_HOST=clickhouse
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "migration-dyndb-clickhouse.py"]