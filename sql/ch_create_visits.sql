-- dds.visits + dds.opentripmap_pois + dds.rest_countries

CREATE TABLE IF NOT EXISTS visits
(
    visit_id UUID,
    user_id UUID,
    tourist_country String,
    place_id String,
    poi_name String,
    poi_category String,
    country_name String,
    visit_date Date,
    rating Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(visit_date)
ORDER BY (visit_date, user_id, place_id);