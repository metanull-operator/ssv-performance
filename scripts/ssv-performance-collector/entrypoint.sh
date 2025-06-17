#!/bin/bash
set -eo pipefail

if [ -n "${CLICKHOUSE_PASSWORD_FILE:-}" ] && [ -f "$CLICKHOUSE_PASSWORD_FILE" ]; then
  export CLICKHOUSE_PASSWORD=$(< "$CLICKHOUSE_PASSWORD_FILE")
fi

if [[ $# -eq 0 ]]; then
  set -- ssv-performance-collector.py
fi

exec gosu collector "$@"