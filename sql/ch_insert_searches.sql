SELECT
    se.search_id,
    se.user_id,
    t.country AS tourist_country,
    se.search_country,
    se.timestamp::date AS search_date,
    CASE WHEN se.converted_to_booking THEN 1 ELSE 0 END AS converted_to_booking
FROM dds.search_events se
LEFT JOIN dds.tourists t ON se.user_id = t.user_id;