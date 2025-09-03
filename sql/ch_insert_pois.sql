SELECT
    p.xid AS place_id,
    p.name AS poi_name,
    p.kinds AS category,
    rc.country_name AS country_name,
    p.description AS description,
    p.image AS image,
    p.rating AS rating,
    p.latitude AS latitude,
    p.longitude AS longitude
FROM dds.opentripmap_pois AS p
LEFT JOIN dds.rest_countries AS rc
    ON p.country_code = rc.country_code;