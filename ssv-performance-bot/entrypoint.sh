#!/bin/bash
set -eo pipefail

# if [ -n "${CLICKHOUSE_PASSWORD_FILE:-}" ] && [ -f "$CLICKHOUSE_PASSWORD_FILE" ]; then
#   export CLICKHOUSE_PASSWORD=$(< "$CLICKHOUSE_PASSWORD_FILE")
# fi

# if [ -n "${DISCORD_TOKEN_FILE:-}" ] && [ -f "$DISCORD_TOKEN_FILE" ]; then
#   export DISCORD_TOKEN=$(< "$DISCORD_TOKEN_FILE")
# fi

# echo "[entrypoint] Arguments received: $@"

# Fallback to default command if none provided
# if [[ $# -eq 0 ]]; then
#   set -- python ssv-performance-bot.py
# fi

# exec gosu bot "$@"

exec python ssv-performance-bot.py "$@"