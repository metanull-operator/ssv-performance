FROM python:3.11-slim

# Build-time args for UID/GID
ARG PROCESS_UID=30003
ARG PROCESS_GID=30003
ARG PROCESS_USER=sheets
ARG PROCESS_GROUP=sheets

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

#ENV CLICKHOUSE_PASSWORD=

# Number of days of performance data to upload to the Google Sheet
#ENV NUMBER_OF_DAYS_TO_UPLOAD=180

# Log level for the sheets script
# Possible values: DEBUG, INFO, WARNING, ERROR, CRITICAL
#ENV SHEETS_LOG_LEVEL=INFO

# Interoperability and logging
ENV GOOGLE_CREDENTIALS_FILE=/google-credentials.json
ENV CLICKHOUSE_PASSWORD_FILE=/clickhouse-password.txt
ENV CLICKHOUSE_HOST=clickhouse
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint.sh"]
CMD []
#CMD ["python", "ssv-validator-count-sheets.py"]