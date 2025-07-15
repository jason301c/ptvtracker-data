-- GTFS Realtime Tables
-- For tracking real-time vehicle positions, trip updates, and service alerts

-- Create realtime schema
CREATE SCHEMA IF NOT EXISTS gtfs_rt;
SET search_path TO gtfs_rt, gtfs, public;

-- Feed message metadata
CREATE TABLE feed_messages (
    feed_message_id SERIAL PRIMARY KEY,
    feed_id VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_id INTEGER NOT NULL REFERENCES gtfs.transport_sources(source_id),
    version_id INTEGER NOT NULL REFERENCES gtfs.versions(version_id),
    incrementality SMALLINT DEFAULT 0, -- 0=full, 1=differential
    UNIQUE(feed_id, timestamp, source_id, version_id)
);

-- Vehicle positions
CREATE TABLE vehicle_positions (
    vehicle_position_id SERIAL PRIMARY KEY,
    feed_message_id INTEGER NOT NULL REFERENCES feed_messages(feed_message_id) ON DELETE CASCADE,
    entity_id VARCHAR(100) NOT NULL,
    vehicle_id VARCHAR(100),
    vehicle_label VARCHAR(100),
    license_plate VARCHAR(50),
    trip_id VARCHAR(100),
    route_id VARCHAR(50),
    direction_id SMALLINT,
    start_time TIME,
    start_date DATE,
    latitude NUMERIC(10,7) NOT NULL,
    longitude NUMERIC(10,7) NOT NULL,
    bearing NUMERIC(5,2),
    odometer NUMERIC(12,2),
    speed NUMERIC(5,2), -- meters/second
    current_stop_sequence INTEGER,
    stop_id VARCHAR(50),
    current_status SMALLINT, -- 0=incoming, 1=stopped, 2=in_transit
    timestamp TIMESTAMP NOT NULL,
    congestion_level SMALLINT, -- 0=unknown, 1=running_smoothly, 2=stop_and_go, 3=congestion, 4=severe_congestion
    occupancy_status SMALLINT, -- 0=empty, 1=many_seats, 2=few_seats, 3=standing_only, 4=crushed, 5=full, 6=not_accepting
    occupancy_percentage INTEGER,
    UNIQUE(feed_message_id, entity_id)
);

-- Trip updates
CREATE TABLE trip_updates (
    trip_update_id SERIAL PRIMARY KEY,
    feed_message_id INTEGER NOT NULL REFERENCES feed_messages(feed_message_id) ON DELETE CASCADE,
    entity_id VARCHAR(100) NOT NULL,
    trip_id VARCHAR(100) NOT NULL,
    route_id VARCHAR(50),
    direction_id SMALLINT,
    start_time TIME,
    start_date DATE,
    schedule_relationship SMALLINT DEFAULT 0, -- 0=scheduled, 1=added, 2=unscheduled, 3=canceled, 5=duplicated
    vehicle_id VARCHAR(100),
    vehicle_label VARCHAR(100),
    timestamp TIMESTAMP,
    delay INTEGER, -- seconds
    UNIQUE(feed_message_id, entity_id)
);

-- Stop time updates (child of trip updates)
CREATE TABLE stop_time_updates (
    stop_time_update_id SERIAL PRIMARY KEY,
    trip_update_id INTEGER NOT NULL REFERENCES trip_updates(trip_update_id) ON DELETE CASCADE,
    stop_sequence INTEGER,
    stop_id VARCHAR(50),
    arrival_delay INTEGER, -- seconds
    arrival_time TIMESTAMP,
    arrival_uncertainty INTEGER,
    departure_delay INTEGER, -- seconds
    departure_time TIMESTAMP,
    departure_uncertainty INTEGER,
    schedule_relationship SMALLINT DEFAULT 0, -- 0=scheduled, 1=skipped, 2=no_data, 3=unscheduled
    stop_time_properties JSON, -- For extensions
    UNIQUE(trip_update_id, COALESCE(stop_sequence, -1), COALESCE(stop_id, ''))
);

-- Service alerts
CREATE TABLE alerts (
    alert_id SERIAL PRIMARY KEY,
    feed_message_id INTEGER NOT NULL REFERENCES feed_messages(feed_message_id) ON DELETE CASCADE,
    entity_id VARCHAR(100) NOT NULL,
    cause SMALLINT, -- 1=unknown, 2=other, 3=technical_problem, etc.
    effect SMALLINT, -- 1=no_service, 2=reduced_service, 3=significant_delays, etc.
    severity_level SMALLINT, -- 1=unknown, 2=info, 3=warning, 4=severe
    url TEXT,
    header_text TEXT,
    description_text TEXT,
    tts_header_text TEXT,
    tts_description_text TEXT,
    UNIQUE(feed_message_id, entity_id)
);

-- Alert active periods
CREATE TABLE alert_active_periods (
    alert_active_period_id SERIAL PRIMARY KEY,
    alert_id INTEGER NOT NULL REFERENCES alerts(alert_id) ON DELETE CASCADE,
    start_time TIMESTAMP,
    end_time TIMESTAMP
);

-- Alert informed entities (what the alert affects)
CREATE TABLE alert_informed_entities (
    informed_entity_id SERIAL PRIMARY KEY,
    alert_id INTEGER NOT NULL REFERENCES alerts(alert_id) ON DELETE CASCADE,
    agency_id VARCHAR(50),
    route_id VARCHAR(50),
    route_type SMALLINT,
    direction_id SMALLINT,
    trip_id VARCHAR(100),
    stop_id VARCHAR(50)
);

-- Create indexes for performance
CREATE INDEX idx_feed_messages_timestamp ON feed_messages(timestamp DESC);
CREATE INDEX idx_feed_messages_source ON feed_messages(source_id, timestamp DESC);
CREATE INDEX idx_feed_messages_version ON feed_messages(version_id, timestamp DESC);
CREATE INDEX idx_feed_messages_active_version ON feed_messages(feed_message_id) 
WHERE version_id IN (SELECT version_id FROM gtfs.versions WHERE is_active = TRUE);

CREATE INDEX idx_vehicle_positions_timestamp ON vehicle_positions(timestamp DESC);
CREATE INDEX idx_vehicle_positions_trip ON vehicle_positions(trip_id) WHERE trip_id IS NOT NULL;
CREATE INDEX idx_vehicle_positions_route ON vehicle_positions(route_id) WHERE route_id IS NOT NULL;
CREATE INDEX idx_vehicle_positions_location ON vehicle_positions USING GIST (ll_to_earth(latitude, longitude));
CREATE INDEX idx_vehicle_positions_feed ON vehicle_positions(feed_message_id);

CREATE INDEX idx_trip_updates_trip ON trip_updates(trip_id);
CREATE INDEX idx_trip_updates_timestamp ON trip_updates(timestamp DESC) WHERE timestamp IS NOT NULL;
CREATE INDEX idx_trip_updates_feed ON trip_updates(feed_message_id);

CREATE INDEX idx_stop_time_updates_trip ON stop_time_updates(trip_update_id);
CREATE INDEX idx_stop_time_updates_stop ON stop_time_updates(stop_id) WHERE stop_id IS NOT NULL;

CREATE INDEX idx_alerts_feed ON alerts(feed_message_id);
CREATE INDEX idx_alerts_severity ON alerts(severity_level);

CREATE INDEX idx_alert_informed_entities_alert ON alert_informed_entities(alert_id);
CREATE INDEX idx_alert_informed_entities_route ON alert_informed_entities(route_id) WHERE route_id IS NOT NULL;
CREATE INDEX idx_alert_informed_entities_stop ON alert_informed_entities(stop_id) WHERE stop_id IS NOT NULL;