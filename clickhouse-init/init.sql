CREATE TABLE IF NOT EXISTS default.operators (
    network String,
    operator_id UInt32,
    operator_name String,
    is_vo UInt8,
    is_private UInt8,
    validator_count UInt32,
    address String,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY network
ORDER BY (network, operator_id);


CREATE TABLE performance (
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

