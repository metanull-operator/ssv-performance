FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV CLICKHOUSE_HOST=clickhouse
ENV CLICKHOUSE_USER=ssv_performance
ENV CLICKHOUSE_PASSWORD=dsFtsxfSfNKd42uDrkp47gt22cfgG8sh

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "ssv-performance-bot.py"]