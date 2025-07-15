-- GTFS Static Tables with Blue-Green Deployment Support
-- Tables structure for Melbourne's Public Transport data

-- All TIMESTAMP columns use TIMESTAMPTZ (UTC).
-- All TIME columns are local to the agency's timezone (see agency.agency_timezone).

-- Create schema
CREATE SCHEMA IF NOT EXISTS gtfs;
SET search_path TO gtfs, public;

-- Versions table for blue-green deployments
CREATE TABLE versions (
    version_id SERIAL PRIMARY KEY,
    version_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- UTC
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- UTC
    is_active BOOLEAN DEFAULT FALSE,
    source_url TEXT,
    description TEXT
);

-- Only one version can be active at a time
CREATE UNIQUE INDEX unique_active_version ON versions (is_active) WHERE is_active = TRUE;

-- Transport sources lookup table
CREATE TABLE transport_sources (
    source_id INTEGER PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL UNIQUE, -- e.g., 'tram', 'train', 'bus'
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP -- UTC
);

-- Agency table
CREATE TABLE agency (
    agency_id VARCHAR(50),
    source_id INTEGER NOT NULL REFERENCES transport_sources(source_id) DEFERRABLE INITIALLY DEFERRED,
    version_id INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    agency_name VARCHAR(255) NOT NULL,
    agency_url VARCHAR(500),
    agency_timezone VARCHAR(50) NOT NULL,
    agency_lang VARCHAR(10),
    agency_fare_url VARCHAR(500),
    PRIMARY KEY (agency_id, source_id, version_id)
);

-- Calendar table (service schedule patterns)
CREATE TABLE calendar (
    service_id VARCHAR(50),
    source_id INTEGER NOT NULL REFERENCES transport_sources(source_id) DEFERRABLE INITIALLY DEFERRED,
    version_id INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    monday SMALLINT CHECK (monday IN (0,1)),
    tuesday SMALLINT CHECK (tuesday IN (0,1)),
    wednesday SMALLINT CHECK (wednesday IN (0,1)),
    thursday SMALLINT CHECK (thursday IN (0,1)),
    friday SMALLINT CHECK (friday IN (0,1)),
    saturday SMALLINT CHECK (saturday IN (0,1)),
    sunday SMALLINT CHECK (sunday IN (0,1)),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    PRIMARY KEY (service_id, source_id, version_id)
);

-- Calendar dates (service exceptions)
CREATE TABLE calendar_dates (
    service_id VARCHAR(50),
    source_id INTEGER NOT NULL REFERENCES transport_sources(source_id) DEFERRABLE INITIALLY DEFERRED,
    version_id INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    date DATE NOT NULL,
    exception_type SMALLINT NOT NULL CHECK (exception_type IN (1,2)), -- 1=added, 2=removed
    PRIMARY KEY (service_id, source_id, version_id, date)
    -- Note: No FK to calendar as service_id can exist in either calendar or calendar_dates
);

-- Levels (for stations with multiple levels)
CREATE TABLE levels (
    level_id VARCHAR(50),
    source_id INTEGER NOT NULL REFERENCES transport_sources(source_id) DEFERRABLE INITIALLY DEFERRED,
    version_id INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    level_index NUMERIC,
    level_name VARCHAR(255),
    PRIMARY KEY (level_id, source_id, version_id)
);

-- Stops table
CREATE TABLE stops (
    stop_id VARCHAR(50),
    source_id INTEGER NOT NULL REFERENCES transport_sources(source_id) DEFERRABLE INITIALLY DEFERRED,
    version_id INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    stop_name VARCHAR(255) NOT NULL,
    stop_lat NUMERIC(10,7),
    stop_lon NUMERIC(10,7),
    location_type SMALLINT DEFAULT 0 CHECK (location_type BETWEEN 0 AND 4),
    parent_station VARCHAR(50),
    wheelchair_boarding SMALLINT DEFAULT 0 CHECK (wheelchair_boarding BETWEEN 0 AND 2),
    level_id VARCHAR(50),
    PRIMARY KEY (stop_id, source_id, version_id),
    FOREIGN KEY (level_id, source_id, version_id) REFERENCES levels(level_id, source_id, version_id) DEFERRABLE INITIALLY DEFERRED
);

-- Add parent station foreign key constraint (deferrable for bulk inserts)
ALTER TABLE stops 
ADD CONSTRAINT fk_stops_parent 
FOREIGN KEY (parent_station, source_id, version_id) REFERENCES stops(stop_id, source_id, version_id)
DEFERRABLE INITIALLY DEFERRED;

-- Routes table
CREATE TABLE routes (
    route_id VARCHAR(50),
    source_id INTEGER NOT NULL REFERENCES transport_sources(source_id) DEFERRABLE INITIALLY DEFERRED,
    version_id INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    agency_id VARCHAR(50),
    route_short_name VARCHAR(50),
    route_long_name VARCHAR(255),
    route_type SMALLINT NOT NULL, -- 0=tram, 1=subway, 2=rail, 3=bus, etc.
    route_color VARCHAR(6),
    route_text_color VARCHAR(6),
    PRIMARY KEY (route_id, source_id, version_id),
    FOREIGN KEY (agency_id, source_id, version_id) REFERENCES agency(agency_id, source_id, version_id) DEFERRABLE INITIALLY DEFERRED
);

-- Shapes table (route geometry)
CREATE TABLE shapes (
    shape_id VARCHAR(50),
    source_id INTEGER NOT NULL REFERENCES transport_sources(source_id) DEFERRABLE INITIALLY DEFERRED,
    version_id INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    shape_pt_lat NUMERIC(10,7) NOT NULL,
    shape_pt_lon NUMERIC(10,7) NOT NULL,
    shape_pt_sequence INTEGER NOT NULL,
    shape_dist_traveled NUMERIC(10,2),
    PRIMARY KEY (shape_id, source_id, version_id, shape_pt_sequence)
);

-- Trips table
CREATE TABLE trips (
    trip_id VARCHAR(100),
    source_id INTEGER NOT NULL REFERENCES transport_sources(source_id) DEFERRABLE INITIALLY DEFERRED,
    version_id INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    route_id VARCHAR(50) NOT NULL,
    service_id VARCHAR(50) NOT NULL,
    shape_id VARCHAR(50),
    trip_headsign VARCHAR(255),
    direction_id SMALLINT CHECK (direction_id IN (0,1)),
    block_id VARCHAR(50),
    wheelchair_accessible SMALLINT DEFAULT 0 CHECK (wheelchair_accessible BETWEEN 0 AND 2),
    PRIMARY KEY (trip_id, source_id, version_id),
    FOREIGN KEY (route_id, source_id, version_id) REFERENCES routes(route_id, source_id, version_id) DEFERRABLE INITIALLY DEFERRED
    -- Note: No FK for service_id as it can exist in either calendar or calendar_dates
    -- Note: No FK for shape_id as some GTFS feeds have trips with shape_ids that don't exist in shapes.txt
);

-- Stop times table (largest table - optimized for queries)
CREATE TABLE stop_times (
    trip_id VARCHAR(100),
    source_id INTEGER NOT NULL REFERENCES transport_sources(source_id) DEFERRABLE INITIALLY DEFERRED,
    version_id INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    stop_id VARCHAR(50) NOT NULL,
    stop_sequence INTEGER NOT NULL,
    arrival_time TIME, -- Local time, interpret in agency's timezone
    departure_time TIME, -- Local time, interpret in agency's timezone
    stop_headsign VARCHAR(255),
    pickup_type SMALLINT DEFAULT 0 CHECK (pickup_type BETWEEN 0 AND 3),
    drop_off_type SMALLINT DEFAULT 0 CHECK (drop_off_type BETWEEN 0 AND 3),
    shape_dist_traveled NUMERIC(10,2),
    PRIMARY KEY (trip_id, source_id, version_id, stop_sequence),
    FOREIGN KEY (trip_id, source_id, version_id) REFERENCES trips(trip_id, source_id, version_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (stop_id, source_id, version_id) REFERENCES stops(stop_id, source_id, version_id) DEFERRABLE INITIALLY DEFERRED
);

-- Pathways table (connections between stops)
CREATE TABLE pathways (
    pathway_id VARCHAR(50),
    source_id INTEGER NOT NULL REFERENCES transport_sources(source_id) DEFERRABLE INITIALLY DEFERRED,
    version_id INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    from_stop_id VARCHAR(50) NOT NULL,
    to_stop_id VARCHAR(50) NOT NULL,
    pathway_mode SMALLINT NOT NULL CHECK (pathway_mode BETWEEN 1 AND 7),
    is_bidirectional SMALLINT CHECK (is_bidirectional IN (0,1)),
    traversal_time INTEGER,
    PRIMARY KEY (pathway_id, source_id, version_id),
    FOREIGN KEY (from_stop_id, source_id, version_id) REFERENCES stops(stop_id, source_id, version_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (to_stop_id, source_id, version_id) REFERENCES stops(stop_id, source_id, version_id) DEFERRABLE INITIALLY DEFERRED
);

-- Transfers table
CREATE TABLE transfers (
    from_stop_id VARCHAR(50),
    to_stop_id VARCHAR(50),
    source_id INTEGER NOT NULL REFERENCES transport_sources(source_id) DEFERRABLE INITIALLY DEFERRED,
    version_id INTEGER NOT NULL REFERENCES versions(version_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    from_route_id VARCHAR(50),
    to_route_id VARCHAR(50),
    from_trip_id VARCHAR(100) NOT NULL DEFAULT '',
    to_trip_id VARCHAR(100) NOT NULL DEFAULT '',
    transfer_type SMALLINT DEFAULT 0 CHECK (transfer_type BETWEEN 0 AND 4),
    min_transfer_time INTEGER,
    PRIMARY KEY (from_stop_id, to_stop_id, source_id, version_id, from_trip_id, to_trip_id),
    FOREIGN KEY (from_stop_id, source_id, version_id) REFERENCES stops(stop_id, source_id, version_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (to_stop_id, source_id, version_id) REFERENCES stops(stop_id, source_id, version_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (from_route_id, source_id, version_id) REFERENCES routes(route_id, source_id, version_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (to_route_id, source_id, version_id) REFERENCES routes(route_id, source_id, version_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (from_trip_id, source_id, version_id) REFERENCES trips(trip_id, source_id, version_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (to_trip_id, source_id, version_id) REFERENCES trips(trip_id, source_id, version_id) DEFERRABLE INITIALLY DEFERRED
);

-- PTV transport source mapping
INSERT INTO transport_sources (source_id, source_name, description) VALUES
(1, 'Regional Train', 'PTV Regional Train Services'),
(2, 'Metropolitan Train', 'PTV Metropolitan Train Network'),
(3, 'Metropolitan Tram', 'PTV Metropolitan Tram Network'),
(4, 'Metropolitan Bus', 'PTV Metropolitan Bus Network'),
(5, 'Regional Coach', 'PTV Regional Coach Services'),
(6, 'Regional Bus', 'PTV Regional Bus Services'),
(10, 'Interstate', 'PTV Interstate Services'),
(11, 'SkyBus', 'PTV SkyBus Airport Services')
ON CONFLICT (source_id) DO UPDATE SET 
    source_name = EXCLUDED.source_name, 
    description = EXCLUDED.description;