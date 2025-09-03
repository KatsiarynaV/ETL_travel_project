CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.rest_countries (
    id SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL,
    capital TEXT,
    country_code CHAR(2) UNIQUE NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    region TEXT,
    subregion TEXT,
    population BIGINT
);

CREATE TABLE IF NOT EXISTS dds.opentripmap_pois (
    id SERIAL PRIMARY KEY,
    xid TEXT UNIQUE NOT NULL,
    name TEXT,
    description TEXT,
    image TEXT,
    kinds TEXT,
    otm_url TEXT,
    country_code CHAR(2) REFERENCES dds.rest_countries(country_code),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    poi_timestamp TIMESTAMP WITH TIME ZONE,
    rating DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dds.tourists (
    user_id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT NOT NULL,
    registration_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.bookings (
    booking_id UUID PRIMARY KEY,
    user_id UUID REFERENCES dds.tourists(user_id),
    destination TEXT NOT NULL,
    type TEXT NOT NULL,
    category TEXT NOT NULL,
    status TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.visits (
    visit_id UUID PRIMARY KEY,
    user_id UUID REFERENCES dds.tourists(user_id),
    place_id TEXT REFERENCES dds.opentripmap_pois(xid),
    timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.search_events (
    search_id UUID PRIMARY KEY,
    user_id UUID REFERENCES dds.tourists(user_id),
    search_country TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    converted_to_booking BOOLEAN NOT NULL
);