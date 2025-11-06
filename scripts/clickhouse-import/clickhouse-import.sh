#!/bin/bash
set -euo pipefail

CLICKHOUSE_USER="${CLICKHOUSE_USER:-ssv_performance}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-default}"
CLICKHOUSE_PASSWORD_FILE="${CLICKHOUSE_PASSWORD_FILE:-/clickhouse-password.txt}"

SQL_DIR="${SQL_DIR:-/sql-import}"

# Load password
if [[ -f "$CLICKHOUSE_PASSWORD_FILE" ]]; then
  export CLICKHOUSE_PASSWORD="$(< "$CLICKHOUSE_PASSWORD_FILE")"
else
  export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
fi

# Helper: unified clickhouse-client invocation
ch() {
  if [[ -n "${CLICKHOUSE_PASSWORD}" ]]; then
    clickhouse-client --host="$CLICKHOUSE_HOST" --user="$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" "$@"
  else
    clickhouse-client --host="$CLICKHOUSE_HOST" --user="$CLICKHOUSE_USER" "$@"
  fi
}

# Verify directory
if [[ ! -d "$SQL_DIR" ]]; then
  echo "SQL import directory not found: $SQL_DIR"
  exit 1
fi

echo "Running SQL scripts from $SQL_DIR..."

# Make *.sql glob empty-safe
shopt -s nullglob

DID_IMPORT_PERFORMANCE=0
sql_files=( "$SQL_DIR"/*.sql )

if [[ ${#sql_files[@]} -eq 0 ]]; then
  echo "No .sql files found in $SQL_DIR"
else
  for file in "${sql_files[@]}"; do
    BASENAME=$(basename "$file")
    TABLENAME="${BASENAME%.sql}"

    echo "Importing $BASENAME into table '$TABLENAME'..."

    # Replace 'INSERT INTO table' with actual table name and pipe into client
    sed "s/INSERT INTO table/INSERT INTO $TABLENAME/" "$file" \
      | ch --database="$CLICKHOUSE_DATABASE"

    echo "Optimizing table '$TABLENAME'..."
    ch --database="$CLICKHOUSE_DATABASE" --query="OPTIMIZE TABLE \`$TABLENAME\` FINAL"

    # Track if we imported into performance table for MV rebuild later
    if [[ "$TABLENAME" == "performance" ]]; then
      DID_IMPORT_PERFORMANCE=1
    fi
  done
fi

# performance_daily is populated by a materialized view that built from performance table.
# No need to export/import it directly, but we do need to rebuild it after importing performance.
# Theoretically this could be done via the MV itself, but to be safe we just repopulate it here.
# This protects against any possible MV issues and ensures data consistency at the cost of rebuilding
# the performance_daily data from scratch.
#
# Conditionally rebuild performance_daily only if we imported into performance
if [[ "$DID_IMPORT_PERFORMANCE" -eq 1 ]]; then
  echo "Detected performance import — rebuilding \`${CLICKHOUSE_DATABASE}\`.performance_daily ..."

  ch --database="$CLICKHOUSE_DATABASE" --multiquery <<SQL
-- Ensure the target table is empty before repopulating
TRUNCATE TABLE \`${CLICKHOUSE_DATABASE}\`.\`performance_daily\`;

-- Repopulate performance_daily from raw performance
INSERT INTO \`${CLICKHOUSE_DATABASE}\`.\`performance_daily\`
SELECT
  network,
  operator_id,
  metric_type,
  metric_date,
  argMax(metric_value, updated_at) AS metric_value,
  max(updated_at)                  AS last_row_at
FROM \`${CLICKHOUSE_DATABASE}\`.\`performance\`
GROUP BY network, operator_id, metric_type, metric_date;
SQL

  echo "Rebuild performance MV complete."
else
  echo "No performance import detected — skipping performance_daily rebuild."
fi

echo "All imports completed."