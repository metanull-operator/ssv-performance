CREATE TABLE IF NOT EXISTS default.operators (
    network String,
    operator_id UInt32,
    operator_name String,
    is_vo UInt8,
    is_private UInt8,
    validator_count Nullable(UInt32),
    operator_fee Nullable(Float64),
    address String,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY network
ORDER BY (network, operator_id);


CREATE TABLE IF NOT EXISTS default.performance (
    network String,
    operator_id UInt32,
    metric_type String,
    metric_date Date,
    metric_value Float64,
    source String,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY network
ORDER BY (network, operator_id, metric_type, metric_date, source);

CREATE TABLE IF NOT EXISTS default.operator_fees (
    network String,
    operator_id UInt32,
    metric_date Date,
    operator_fee Float64,
    source String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY network
ORDER BY (network, operator_id, metric_date, source);

CREATE TABLE IF NOT EXISTS default.validator_counts (
    network String,
    operator_id UInt32,
    metric_date Date,
    validator_count UInt32,
    source String,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY network
ORDER BY (network, operator_id, metric_date, source);

CREATE TABLE IF NOT EXISTS default.subscriptions (
    network String,
    user_id UInt64,
    operator_id UInt32,
    subscription_type String,
    enabled UInt8 DEFAULT 1,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY network
ORDER BY (network, user_id, operator_id, subscription_type);

CREATE TABLE IF NOT EXISTS default.import_state (
    network String,
    last_successful_import DateTime,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY network;

-- base tables: keep names
--   operators
--   validator_counts  (raw time series)

-- precomputed targets must have different names than the source:
CREATE TABLE IF NOT EXISTS validator_counts_latest
(
  network LowCardinality(String),
  operator_id UInt32,
  validator_count UInt32,
  counts_latest_at DateTime
)
ENGINE = MergeTree
ORDER BY (network, operator_id);

CREATE TABLE IF NOT EXISTS validator_counts_daily
(
  network LowCardinality(String),
  operator_id UInt32,
  metric_date Date,
  validator_count UInt32,
  last_row_at DateTime
)
ENGINE = ReplacingMergeTree(last_row_at)
ORDER BY (network, operator_id, metric_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS validator_counts_latest_mv
TO validator_counts_latest AS
SELECT
  network,
  operator_id,
  argMax(validator_count, updated_at) AS validator_count,
  max(updated_at)                     AS counts_latest_at
FROM validator_counts
GROUP BY network, operator_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS validator_counts_daily_mv
TO validator_counts_daily AS
SELECT
  network,
  operator_id,
  metric_date,
  argMax(validator_count, updated_at) AS validator_count,
  max(updated_at)                     AS last_row_at
FROM validator_counts
GROUP BY network, operator_id, metric_date;
