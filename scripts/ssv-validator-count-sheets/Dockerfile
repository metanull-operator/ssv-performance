FROM python:3.11-slim

# Ethereum network to use
# Possible values: mainnet, holesky, hoodi
ENV NETWORK=mainnet

# Number of days of performance data to upload to the Google Sheet
ENV NUMBER_OF_DAYS_TO_UPLOAD=180

ENV GOOGLE_CREDENTIALS_FILE=/google-credentials.json

# ClickHouse connection details
ENV CLICKHOUSE_HOST=clickhouse
ENV CLICKHOUSE_USER=ssv_performance
ENV CLICKHOUSE_PASSWORD_FILE=/clickhouse-password.txt

# Log level for the sheets script
# Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
ENV SHEETS_LOG_LEVEL=INFO

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
CMD []