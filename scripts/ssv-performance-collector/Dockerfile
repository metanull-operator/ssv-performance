FROM python:3.11-slim

# Build-time args for UID/GID
ARG PROCESS_UID=30002
ARG PROCESS_GID=30002
ARG PROCESS_USER=collector
ARG PROCESS_GROUP=collector

# Install dependencies and gosu for privilege dropping
RUN apt-get update && apt-get install -y --no-install-recommends \
    gosu \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user and group safely
RUN set -eux; \
    if ! getent group "${PROCESS_GID}" >/dev/null; then \
        groupadd -g "${PROCESS_GID}" "${PROCESS_GROUP}"; \
    fi; \
    if ! id -u "${PROCESS_UID}" >/dev/null 2>&1; then \
        useradd -u "${PROCESS_UID}" -g "${PROCESS_GID}" -m -s /bin/bash "${PROCESS_USER}"; \
    fi

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN chmod +x /app/entrypoint.sh

ENV CLICKHOUSE_PASSWORD=

# Number of outbound API calls per minute
#ENV REQUESTS_PER_MINUTE=20

# Log level for the collector
# Possible values: DEBUG, INFO, WARNING, ERROR, CRITICAL
#ENV COLLECTOR_LOG_LEVEL=INFO

# Number of days of 24h performance data to show with /operator command
#ENV OPERATOR_PERFORMANCE_DAYS=7

# Interoperability and logging
ENV CLICKHOUSE_PASSWORD_FILE=/etc/ssv-performance-bot/clickhouse-password.txt
ENV CLICKHOUSE_HOST=clickhouse
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["python", "ssv-performance-collector.py"]