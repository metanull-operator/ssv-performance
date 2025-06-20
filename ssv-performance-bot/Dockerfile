FROM python:3.11-slim

ENV DISCORD_TOKEN_FILE=/discord-token.txt

ENV CLICKHOUSE_HOST=clickhouse
ENV CLICKHOUSE_USER=ssv_performance
ENV CLICKHOUSE_PASSWORD_FILE=/clickhouse-password.txt

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
CMD []