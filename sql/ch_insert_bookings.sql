SELECT
    b.booking_id,
    b.user_id,
    t.name AS tourist_name,
    t.country AS tourist_country,
    b.start_date AS booking_date,
    b.destination,
    b.type AS booking_type,
    b.category,
    b.status,
    b.price::float8 AS price,
    b.start_date,
    b.end_date
FROM dds.bookings b
LEFT JOIN dds.tourists t ON b.user_id = t.user_id
