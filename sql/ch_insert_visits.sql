SELECT
    v.visit_id,
    v.user_id,
    t.country AS tourist_country,
    v.place_id,
    p.name AS poi_name,
    p.kinds AS poi_category,
    rc.country_name,
    v.timestamp::date AS visit_date,
    p.rating AS rating
FROM dds.visits v
LEFT JOIN dds.tourists t ON v.user_id = t.user_id
LEFT JOIN dds.opentripmap_pois p ON v.place_id = p.xid
LEFT JOIN dds.rest_countries rc ON p.country_code = rc.country_code;