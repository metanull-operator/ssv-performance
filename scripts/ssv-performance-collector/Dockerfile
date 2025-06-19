FROM python:3.11-slim

WORKDIR /app

COPY . .
RUN pip install --no-cache-dir -r requirements.txt
RUN chmod +x /app/entrypoint.sh

ENV CLICKHOUSE_PASSWORD_FILE=/clickhouse-password.txt
ENV CLICKHOUSE_HOST=clickhouse
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/entrypoint.sh"]
CMD []