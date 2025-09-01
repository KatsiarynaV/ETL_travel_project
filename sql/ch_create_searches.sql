-- dds.search_events + dds.tourists

CREATE TABLE IF NOT EXISTS searches
(
    search_id UUID,
    user_id UUID,
    tourist_country String,
    search_country String,
    search_date Date,
    converted_to_booking UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(search_date)
ORDER BY (search_date, user_id);