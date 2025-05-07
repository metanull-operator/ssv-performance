#!/bin/bash
set -e

# Load password securely from file
if [[ -f "$CLICKHOUSE_PASSWORD_FILE" ]]; then
  export CLICKHOUSE_PASSWORD=$(< "$CLICKHOUSE_PASSWORD_FILE")
else
  export CLICKHOUSE_PASSWORD=""
fi

# Run healthcheck with no password on CLI
clickhouse-client \
  --host="${CLICKHOUSE_HOST:-clickhouse}" \
  --user="${CLICKHOUSE_USER:-ssv_performance}" \
  --query="SELECT 1" \
  > /dev/null
