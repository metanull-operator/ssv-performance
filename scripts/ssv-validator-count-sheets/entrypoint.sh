#!/bin/bash
set -eo pipefail

if [ -n "${CLICKHOUSE_PASSWORD_FILE:-}" ] && [ -f "$CLICKHOUSE_PASSWORD_FILE" ]; then
  export CLICKHOUSE_PASSWORD=$(< "$CLICKHOUSE_PASSWORD_FILE")
fi

# If no arguments passed, use default command from environment
if [[ $# -eq 0 && -n "$DEFAULT_CMD" ]]; then
  set -- $DEFAULT_CMD
fi

# If first argument starts with "-", assume it's flags for the default command
if [[ "$1" =~ ^- && -n "$DEFAULT_CMD" ]]; then
  set -- $DEFAULT_CMD "$@"
fi

exec gosu "${PROCESS_USER:-app}" "$@"