#!/bin/bash

# Healthcheck script for ClickHouse
# This script checks if ClickHouse is reachable and responsive
# Used in Docker healthcheck

# Exit immediately if a command exits with a non-zero status
set -e

echo "[healthcheck] Running ClickHouse healthcheck at $(date)"

# Debug info: file ownership and permissions
if [[ -f "$CLICKHOUSE_PASSWORD_FILE" ]]; then
  echo "[healthcheck] Found password file: $CLICKHOUSE_PASSWORD_FILE"
  echo "[healthcheck] File permissions: $(stat -c '%A %U:%G' "$CLICKHOUSE_PASSWORD_FILE")"
  echo "[healthcheck] Current user: $(id -u), group: $(id -g)"
  
  # Load password
  export CLICKHOUSE_PASSWORD=$(< "$CLICKHOUSE_PASSWORD_FILE")
else
  echo "[healthcheck] Password file not found: $CLICKHOUSE_PASSWORD_FILE" >&2
  export CLICKHOUSE_PASSWORD=""
fi

# Run healthcheck query
clickhouse-client \
  --host="${CLICKHOUSE_HOST:-clickhouse}" \
  --user="${CLICKHOUSE_USER:-ssv_performance}" \
  --query="SELECT 1" \
  > /dev/null || {
    echo "[healthcheck] ClickHouse healthcheck failed with exit code $?" >&2
    exit 1
}

echo "[healthcheck] ClickHouse is healthy"
