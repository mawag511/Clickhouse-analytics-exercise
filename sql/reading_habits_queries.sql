-- Time spent reading statistics
WITH stat_count AS (
    SELECT
        avg(duration_minutes) AS average_x_session
        , min(duration_minutes) AS shortest_session
        , max(duration_minutes) AS longest_session
        , count(*) AS total_sessions
    FROM my_db.reading_habits
)
SELECT
    formatDateTime(
        toDateTime(0, 'UTC')
        + INTERVAL average_x_session MINUTE
        , '%T'
    ) AS avg_session_time
    , formatDateTime(
        toDateTime(0, 'UTC')
        + INTERVAL shortest_session MINUTE
        , '%T'
    ) AS shortest_session_time
    , formatDateTime(
        toDateTime(0, 'UTC')
        + INTERVAL longest_session MINUTE
        , '%T'
    ) AS longest_session_time
    , total_sessions
FROM stat_count;
 
 
 
-- Total reading time
SELECT
    formatReadableTimeDelta(sum(duration_minutes) * 60, 'hours') AS total_time_reading
FROM my_db.reading_habits;
 
 
 
-- TOP 3 genres
SELECT
    genre
    , sum(duration_minutes) AS total_minutes
FROM my_db.reading_habits
GROUP BY genre
ORDER BY total_minutes DESC
LIMIT 3;
 
 
 
-- Weekly analysis
SELECT
    toMonday(date) AS week
    , sum(duration_minutes) AS total_minutes
FROM my_db.reading_habits
GROUP BY week;
 
 
 
-- Literature ratings
SELECT
    title
    , round(avg(rating), 2) AS average_rating
FROM my_db.reading_habits
GROUP BY title
ORDER BY average_rating DESC;