FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV CLICKHOUSE_HOST=clickhouse
ENV CLICKHOUSE_USER=ssv_performance
ENV CLICKHOUSE_PASSWORD=dsFtsxfSfNKd42uDrkp47gt22cfgG8sh

ENV PYTHONUNBUFFERED=1

ENV BOT_LOG_LEVEL=INFO
ENV BOT_DAILY_MESSAGE_TIME="1:00"
ENV BOT_EXTRA_MESSAGE="\n/subscribe for @mentions on daily alerts or for performance report DMs. /operator for latest performance data. /help for more info."

ENTRYPOINT ["python", "ssv-performance-bot.py"]