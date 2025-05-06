#!/bin/bash
set -euo pipefail

# If CLICKHOUSE_PASSWORD_FILE is set and the file exists
if [ -n "$CLICKHOUSE_PASSWORD_FILE" ] && [ -f "$CLICKHOUSE_PASSWORD_FILE" ]; then
  export CLICKHOUSE_PASSWORD=$(< "$CLICKHOUSE_PASSWORD_FILE")
fi

echo "[entrypoint] Arguments received: $@"

# Fallback to default command if none provided
if [[ $# -eq 0 ]]; then
  set -- python ssv-performance-bot.py
fi

exec gosu bot "$@"
