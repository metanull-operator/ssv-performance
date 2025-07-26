#!/bin/bash
set -euo pipefail

CLICKHOUSE_USER="${CLICKHOUSE_USER:-ssv_performance}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-default}"
CLICKHOUSE_PASSWORD_FILE="${CLICKHOUSE_PASSWORD_FILE:-/clickhouse-password.txt}"

EXPORT_BASE_DIR="${EXPORT_BASE_DIR:-/sql-export}"
DATE_DIR=$(date '+%Y-%m-%d_%H-%M-%S')
EXPORT_DIR="${EXPORT_BASE_DIR}/${DATE_DIR}"

# Load password
if [[ -f "$CLICKHOUSE_PASSWORD_FILE" ]]; then
  export CLICKHOUSE_PASSWORD=$(< "$CLICKHOUSE_PASSWORD_FILE")
else
  export CLICKHOUSE_PASSWORD=""
fi

# Create export directory
mkdir -p "$EXPORT_DIR"

# Get list of tables
TABLES=$(clickhouse-client \
  --host="$CLICKHOUSE_HOST" \
  --user="$CLICKHOUSE_USER" \
  --database="$CLICKHOUSE_DATABASE" \
  --query="SHOW TABLES")

echo "📤 Exporting tables from database '$CLICKHOUSE_DATABASE' into '$EXPORT_DIR'..."

for TABLE in $TABLES; do
  OUTPUT_FILE="${EXPORT_DIR}/${TABLE}.sql"
  echo "→ Exporting table '$TABLE' to $OUTPUT_FILE..."

  clickhouse-client \
    --host="$CLICKHOUSE_HOST" \
    --user="$CLICKHOUSE_USER" \
    --database="$CLICKHOUSE_DATABASE" \
    --query="SELECT * FROM $TABLE FORMAT SQLInsert" \
    > "$OUTPUT_FILE"
done

echo "✅ All tables exported to $EXPORT_DIR"
