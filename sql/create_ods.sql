CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.rest_countries (
    id SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL,
    capital TEXT,
    country_code CHAR(2) NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    region TEXT,
    subregion TEXT,
    population BIGINT
);

CREATE TABLE IF NOT EXISTS ods.opentripmap_pois (
    id SERIAL PRIMARY KEY,
    xid TEXT NOT NULL,
    name TEXT,
    description TEXT,
    image TEXT,
    kinds TEXT,
    otm_url TEXT,
    country_code CHAR(2),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    poi_timestamp TIMESTAMP WITH TIME ZONE,
    rating DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS ods.tourists (
    user_id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT NOT NULL,
    registration_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS ods.bookings (
    booking_id UUID PRIMARY KEY,
    user_id UUID REFERENCES ods.tourists(user_id),
    destination TEXT NOT NULL,
    type TEXT NOT NULL,
    category TEXT NOT NULL,
    status TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS ods.visits (
    visit_id UUID PRIMARY KEY,
    user_id UUID REFERENCES ods.tourists(user_id),
    place_id UUID NOT NULL,
    timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS ods.search_events (
    search_id UUID PRIMARY KEY,
    user_id UUID REFERENCES ods.tourists(user_id),
    search_country TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    converted_to_booking BOOLEAN NOT NULL
);