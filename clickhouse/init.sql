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

CREATE TABLE IF NOT EXISTS performance_daily
(
  network       LowCardinality(String),
  operator_id   UInt32,
  metric_type   LowCardinality(String),
  metric_date   Date,
  metric_value  Float64,
  last_row_at   DateTime
)
ENGINE = ReplacingMergeTree(last_row_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (network, metric_type, operator_id, metric_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS performance_daily_mv
TO performance_daily AS
SELECT
  network,
  operator_id,
  metric_type,
  metric_date,
  argMax(metric_value, updated_at) AS metric_value,
  max(updated_at)                  AS last_row_at
FROM performance
GROUP BY network, operator_id, metric_type, metric_date;