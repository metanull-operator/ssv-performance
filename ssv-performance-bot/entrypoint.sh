#!/bin/bash
set -e

# If CLICKHOUSE_PASSWORD_FILE is set and the file exists
if [ -n "$CLICKHOUSE_PASSWORD_FILE" ] && [ -f "$CLICKHOUSE_PASSWORD_FILE" ]; then
  export CLICKHOUSE_PASSWORD=$(< "$CLICKHOUSE_PASSWORD_FILE")
fi

# Exec the original entrypoint
exec python ssv-performance-bot.py "$@"