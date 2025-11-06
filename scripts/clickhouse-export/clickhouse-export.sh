#!/bin/bash
set -euo pipefail

CLICKHOUSE_USER="${CLICKHOUSE_USER:-ssv_performance}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-default}"
CLICKHOUSE_PASSWORD_FILE="${CLICKHOUSE_PASSWORD_FILE:-/clickhouse-password.txt}"
DAYS_TO_KEEP="${DAYS_TO_KEEP:-0}"

EXPORT_BASE_DIR="${EXPORT_BASE_DIR:-/sql-export}"
DATE_DIR=$(date '+%Y-%m-%d_%H-%M-%S')
EXPORT_DIR="${EXPORT_BASE_DIR}/${DATE_DIR}"

# 🔒 HARD-CODED TABLE LIST
# Edit this array to control what gets exported.
# Keep only "real" tables here — not materialized views.
ALLOWED_TABLES=(
  operators
  operator_fees
  validator_counts
  performance
)

# Load password
if [[ -f "$CLICKHOUSE_PASSWORD_FILE" ]]; then
  export CLICKHOUSE_PASSWORD="$(< "$CLICKHOUSE_PASSWORD_FILE")"
else
  export CLICKHOUSE_PASSWORD=""
fi

# helper to call clickhouse-client with/without password
ch() {
  if [[ -n "${CLICKHOUSE_PASSWORD}" ]]; then
    clickhouse-client --host="$CLICKHOUSE_HOST" --user="$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" "$@"
  else
    clickhouse-client --host="$CLICKHOUSE_HOST" --user="$CLICKHOUSE_USER" "$@"
  fi
}

# Create export directory
mkdir -p "$EXPORT_DIR"

echo "Exporting selected tables from database '$CLICKHOUSE_DATABASE' into '$EXPORT_DIR'..."
echo "Tables: ${ALLOWED_TABLES[*]}"

for TABLE in "${ALLOWED_TABLES[@]}"; do
  # Check that the table actually exists
  EXISTS=$(ch --query="SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}" \
              --param_db="$CLICKHOUSE_DATABASE" \
              --param_tbl="$TABLE")
  if [[ "$EXISTS" -eq 0 ]]; then
    echo "Skipping '$TABLE' — not found in database '$CLICKHOUSE_DATABASE'."
    continue
  fi

  # (Optional) we can look at engine and refuse views/MVs just in case:
  ENGINE=$(ch --query="SELECT engine FROM system.tables WHERE database = {db:String} AND name = {tbl:String} LIMIT 1" \
              --param_db="$CLICKHOUSE_DATABASE" \
              --param_tbl="$TABLE")

  case "$ENGINE" in
    *MaterializedView*|*View*|*LiveView*|*Dictionary*)
      echo "Skipping '$TABLE' (engine: $ENGINE) — not exporting non-table objects."
      continue
      ;;
  esac

  OUTPUT_FILE="${EXPORT_DIR}/${TABLE}.sql"
  echo "Exporting table '$TABLE' (engine: $ENGINE) to $OUTPUT_FILE..."

  ch --database="$CLICKHOUSE_DATABASE" \
     --query="SELECT * FROM \`$TABLE\` FORMAT SQLInsert" \
     > "$OUTPUT_FILE"
done

echo "All selected tables exported to $EXPORT_DIR"

# Cleanup old backups using folder names instead of mtime
if [[ "$DAYS_TO_KEEP" -gt 0 ]]; then
  echo "Deleting backups older than $DAYS_TO_KEEP day(s) based on folder name..."

  CUTOFF_DATE=$(date -d "$DAYS_TO_KEEP days ago" +%s)

  for dir in "$EXPORT_BASE_DIR"/*/; do
    dir=${dir%/}  # remove trailing slash
    basename=$(basename "$dir")

    # Extract date part (assumes format YYYY-MM-DD_HH-MM-SS)
    dir_date_part="${basename%%_*}"

    # Skip if not a valid date format
    if ! date_ts=$(date -d "$dir_date_part" +%s 2>/dev/null); then
      echo "Skipping unrecognized folder: $basename"
      continue
    fi

    if [[ "$date_ts" -lt "$CUTOFF_DATE" ]]; then
      echo "Deleting old backup: $basename"
      rm -rf "$dir"
    fi
  done

  echo "Old backups cleaned."
fi
