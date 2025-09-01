CREATE TABLE IF NOT EXISTS tourists
(
    user_id UUID,
    name String,
    country String,
    registration_date Date
)
ENGINE = MergeTree
ORDER BY (user_id);