-- dds.bookings + dds.tourists + dds.rest_countries

CREATE TABLE IF NOT EXISTS bookings
(
    booking_id UUID,
    user_id UUID,
    tourist_name String,
    tourist_country String,
    booking_date Date,
    destination String,
    booking_type String,
    category String,
    status String,
    price Float64,
    start_date Date,
    end_date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(booking_date)
ORDER BY (booking_date, user_id);