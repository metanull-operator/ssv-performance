FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Additional text that will be sent with some bot messages
ENV BOT_EXTRA_MESSAGE="/subscribe for @mentions on daily alerts or for performance report DMs. /operator for latest performance data. /help for more info."

ENV CLICKHOUSE_PASSWORD_FILE=/etc/ssv-performance-bot/clickhouse-password.txt

# Time when the bot will send daily messages
# Format: HH:MM (24-hour format)
#ENV BOT_DAILY_MESSAGE_TIME="08:00"

# Log level for the bot
# Possible values: DEBUG, INFO, WARNING, ERROR, CRITICAL
#ENV BOT_LOG_LEVEL=INFO

ENV CLICKHOUSE_HOST=clickhouse
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "ssv-performance-bot.py"]