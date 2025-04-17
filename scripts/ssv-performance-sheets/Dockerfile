FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV CLICKHOUSE_PASSWORD=

# Number of days of performance data to upload to the Google Sheet
#ENV NUMBER_OF_DAYS_TO_UPLOAD=180

# Log level for the sheets script
# Possible values: DEBUG, INFO, WARNING, ERROR, CRITICAL
#ENV SHEETS_LOG_LEVEL=INFO

# Interoperability and logging
ENV GOOGLE_CREDENTIALS_FILE=/google-credentials.json
ENV CLICKHOUSE_HOST=clickhouse
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "ssv-performance-sheets.py"]