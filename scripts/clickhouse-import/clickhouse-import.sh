#!/bin/bash
set -euo pipefail

CLICKHOUSE_USER="${CLICKHOUSE_USER:-ssv_performance}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-default}"
CLICKHOUSE_PASSWORD_FILE="${CLICKHOUSE_PASSWORD_FILE:-/etc/ssv-performance-bot/clickhouse-password.txt}"
SQL_DIR="${SQL_DIR:-/sql-import}"

# Load password
if [[ -f "$CLICKHOUSE_PASSWORD_FILE" ]]; then
  export CLICKHOUSE_PASSWORD=$(< "$CLICKHOUSE_PASSWORD_FILE")
else
  export CLICKHOUSE_PASSWORD=""
fi

# Verify directory
if [[ ! -d "$SQL_DIR" ]]; then
  echo "❌ SQL import directory not found: $SQL_DIR"
  exit 1
fi

echo "📥 Running SQL scripts from $SQL_DIR..."

for file in "$SQL_DIR"/*.sql; do
  BASENAME=$(basename "$file")
  TABLENAME="${BASENAME%.sql}"

  echo "→ Importing $BASENAME into table '$TABLENAME'..."

  # Replace 'INSERT INTO table' with actual table name and pipe into client
  sed "s/INSERT INTO table/INSERT INTO $TABLENAME/" "$file" \
    | clickhouse-client \
        --host="$CLICKHOUSE_HOST" \
        --user="$CLICKHOUSE_USER" \
        --database="$CLICKHOUSE_DATABASE"

done

echo "✅ All imports completed."
