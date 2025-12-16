my_db TABLE my_db.server_metrics
(
    timestamp DateTime,
    metric_name String,
    metric_value Float64,
    host String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (metric_name, host, timestamp);