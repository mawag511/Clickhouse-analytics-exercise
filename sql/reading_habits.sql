CREATE TABLE my_db.reading_habits
(
    date Date,
    start_time DateTime,
    end_time DateTime,
    duration_minutes UInt32,
    title String,
    author String,
    type Enum('book' = 1, 'article' = 2, 'paper' = 3),
    genre String,
    rating UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, genre, author);