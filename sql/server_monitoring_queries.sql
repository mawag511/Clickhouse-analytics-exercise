-- Average CPU usage every 5 minutes (host1)
SELECT
    CASE
        WHEN timestamp = toDateTime('2025-11-25 12:59:30') THEN toDateTime('2025-11-25 12:59:30')
        ELSE toStartOfInterval(timestamp, INTERVAL 5 MINUTES)
    END AS interval_ts
    , round(avg(metric_value), 2) AS average_cpu_load
FROM my_db.server_metrics
WHERE metric_name = 'CPU load' AND host = '192.168.45.23'
GROUP BY interval_ts
ORDER BY interval_ts;
 
-- Average CPU usage every 5 minutes (host2)
SELECT
    CASE
        WHEN timestamp = toDateTime('2025-11-25 12:59:30') THEN toDateTime('2025-11-25 12:59:30')
        ELSE toStartOfInterval(timestamp, INTERVAL 5 MINUTES)
    END AS interval_ts
    , round(avg(metric_value), 2) AS average_cpu_load
FROM my_db.server_metrics
WHERE metric_name = 'CPU load' AND host = '192.169.60.30'
GROUP BY interval_ts
ORDER BY interval_ts;
 
 
 
-- Disk usage (host 1)
SELECT
    toStartOfInterval(timestamp, INTERVAL 20 MINUTES) AS interval_ts
    , max(metric_value) AS peak_usage
    , min(metric_value) AS minimum_usage
    , round(avg(metric_value), 2) AS average_usage
FROM my_db.server_metrics
WHERE metric_name = 'Disk load' AND host = '192.168.45.23'
GROUP BY interval_ts
ORDER BY interval_ts;
 
-- Disk usage (host 2)
SELECT
    toStartOfInterval(timestamp, INTERVAL 20 MINUTES) AS interval_ts
    , max(metric_value) AS peak_usage
    , min(metric_value) AS minimum_usage
    , round(avg(metric_value), 2) AS average_usage
FROM my_db.server_metrics
WHERE metric_name = 'Disk load' AND host = '192.169.60.30'
GROUP BY interval_ts
ORDER BY interval_ts;
 
 
 
-- Anomalies
WITH cpu_overload AS (
    SELECT
        timestamp
        , metric_value AS cpu_load
        , host
    FROM my_db.server_metrics
    WHERE metric_value BETWEEN 75 AND 100 AND metric_name = 'CPU load'
),
disk_overload AS (
    SELECT
        timestamp
        , metric_value AS disk_load
        , host
    FROM my_db.server_metrics
    WHERE metric_value BETWEEN 75 AND 100 AND metric_name = 'Disk load'
),
memory_overload AS (
    SELECT
        timestamp
        , metric_value AS memory_load
        , host
    FROM my_db.server_metrics
    WHERE metric_value BETWEEN 75 AND 100 AND metric_name = 'Memory load'
)
SELECT
    timestamp
    , cpu_load
    , disk_load
    , memory_load
    , host
FROM cpu_overload
JOIN disk_overload USING (timestamp, host)
JOIN memory_overload USING (timestamp, host)
ORDER BY timestamp;
 
 
 
-- Peak memory load
SELECT
    host
    , arrayMax(groupArray(metric_value)) AS peak_load
FROM my_db.server_metrics
WHERE metric_name = 'Memory load'
  AND host IN ('192.168.45.23', '192.169.60.30')
GROUP BY host;