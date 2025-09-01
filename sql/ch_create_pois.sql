CREATE TABLE IF NOT EXISTS pois
(
    place_id String,
    poi_name String,
    category String,
    country_name String,
    description String,
    image String,
    rating Float64,
    latitude Float64,
    longitude Float64
)
ENGINE = MergeTree
ORDER BY (place_id);