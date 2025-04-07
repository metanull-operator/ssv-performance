FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV GOOGLE_CREDENTIALS_FILE=/google-credentials.json

ENV CLICKHOUSE_HOST=clickhouse
ENV CLICKHOUSE_USER=ssv_performance
ENV CLICKHOUSE_PASSWORD=dsFtsxfSfNKd42uDrkp47gt22cfgG8sh

ENV NUMBER_OF_DAYS_TO_UPLOAD=180

ENV SHEETS_LOG_LEVEL=INFO

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "ssv-performance-sheets.py"]