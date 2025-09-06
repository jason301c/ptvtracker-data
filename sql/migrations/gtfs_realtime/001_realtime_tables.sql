-- GTFS Realtime Tables
-- For tracking real-time vehicle positions, trip updates, and service alerts
-- Based on Victorian GTFS-realtime API endpoints

-- All TIMESTAMP columns use TIMESTAMPTZ (UTC).
-- All TIME columns are local to the agency's timezone (see gtfs.agency.agency_timezone).
-- All BIGINT Unix timestamps are always UTC.

-- Create realtime schema
CREATE SCHEMA IF NOT EXISTS gtfs_rt;
SET search_path TO gtfs_rt, gtfs, public;

-- Feed message metadata
CREATE TABLE IF NOT EXISTS feed_messages (
    feed_message_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL, -- UTC
    gtfs_realtime_version VARCHAR(10),
    incrementality SMALLINT DEFAULT 0, -- 0=FULL_DATASET, 1=DIFFERENTIAL
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, -- UTC
    source_id INTEGER NOT NULL REFERENCES gtfs.transport_sources(source_id),
    version_id INTEGER NOT NULL REFERENCES gtfs.versions(version_id),
    feed_type VARCHAR(20) NOT NULL, -- 'vehicle_positions', 'trip_updates', 'service_alerts'
    UNIQUE(received_at, source_id, version_id, feed_type)
);

-- Vehicle positions
CREATE TABLE IF NOT EXISTS vehicle_positions (
    vehicle_position_id SERIAL PRIMARY KEY,
    feed_message_id INTEGER NOT NULL REFERENCES feed_messages(feed_message_id) ON DELETE CASCADE,
    entity_id VARCHAR(100) NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    -- Trip descriptor
    trip_id VARCHAR(100),
    route_id VARCHAR(50),
    start_time INTEGER, -- Seconds since midnight (can exceed 86400 for next-day services)
    start_date DATE,
    schedule_relationship SMALLINT DEFAULT 0, -- 0=SCHEDULED, 1=ADDED, 2=UNSCHEDULED, 3=CANCELED, 5=DUPLICATED, 6=DELETED
    -- Vehicle descriptor
    vehicle_id VARCHAR(100),
    vehicle_label VARCHAR(100),
    license_plate VARCHAR(50),
    -- Position
    latitude NUMERIC(10,7) NOT NULL,
    longitude NUMERIC(10,7) NOT NULL,
    bearing NUMERIC(5,2),
    -- Status
    current_status SMALLINT, -- 0=INCOMING_AT, 1=STOPPED_AT, 2=IN_TRANSIT_TO
    stop_id VARCHAR(50),
    timestamp TIMESTAMPTZ NOT NULL, -- UTC
    UNIQUE(feed_message_id, entity_id)
);

-- Trip updates
CREATE TABLE IF NOT EXISTS trip_updates (
    trip_update_id SERIAL PRIMARY KEY,
    feed_message_id INTEGER NOT NULL REFERENCES feed_messages(feed_message_id) ON DELETE CASCADE,
    entity_id VARCHAR(100) NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    -- Trip descriptor
    trip_id VARCHAR(100) NOT NULL,
    route_id VARCHAR(50),
    direction_id SMALLINT,
    start_time INTEGER, -- Seconds since midnight (can exceed 86400 for next-day services)
    start_date DATE,
    schedule_relationship SMALLINT DEFAULT 0, -- 0=SCHEDULED, 1=ADDED, 2=UNSCHEDULED, 3=CANCELED, 5=DUPLICATED, 6=DELETED
    -- Vehicle descriptor
    vehicle_id VARCHAR(100),
    vehicle_label VARCHAR(100),
    -- Update fields
    timestamp TIMESTAMPTZ, -- UTC
    delay INTEGER, -- seconds
    UNIQUE(feed_message_id, entity_id)
);

-- Stop time updates (child of trip updates)
CREATE TABLE IF NOT EXISTS stop_time_updates (
    stop_time_update_id SERIAL PRIMARY KEY,
    trip_update_id INTEGER NOT NULL REFERENCES trip_updates(trip_update_id) ON DELETE CASCADE,
    stop_sequence INTEGER NOT NULL DEFAULT -1,
    stop_id VARCHAR(50) NOT NULL DEFAULT '',
    -- Arrival info
    arrival_delay INTEGER, -- seconds (positive = late, negative = early)
    arrival_time BIGINT, -- POSIX time, always UTC
    arrival_uncertainty INTEGER,
    -- Departure info
    departure_delay INTEGER, -- seconds
    departure_time BIGINT, -- POSIX time, always UTC
    departure_uncertainty INTEGER,
    -- Schedule relationship
    schedule_relationship SMALLINT DEFAULT 0, -- 0=SCHEDULED, 1=ADDED, 2=UNSCHEDULED, 3=CANCELED, 5=DUPLICATED, 6=DELETED
    UNIQUE(trip_update_id, stop_sequence, stop_id)
);

-- Service alerts
CREATE TABLE IF NOT EXISTS alerts (
    alert_id SERIAL PRIMARY KEY,
    feed_message_id INTEGER NOT NULL REFERENCES feed_messages(feed_message_id) ON DELETE CASCADE,
    entity_id VARCHAR(100) NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    -- Alert fields
    cause SMALLINT, -- UNKNOWN_CAUSE, OTHER_CAUSE, TECHNICAL_PROBLEM, STRIKE, DEMONSTRATION, ACCIDENT, HOLIDAY, WEATHER, MAINTENANCE, CONSTRUCTION, POLICE_ACTIVITY, MEDICAL_EMERGENCY
    effect SMALLINT, -- NO_SERVICE, REDUCED_SERVICE, SIGNIFICANT_DELAYS, DETOUR, ADDITIONAL_SERVICE, MODIFIED_SERVICE, OTHER_EFFECT, UNKNOWN_EFFECT, STOP_MOVED, NO_EFFECT, ACCESSIBILITY_ISSUE
    severity SMALLINT, -- UNKNOWN_SEVERITY, INFO, WARNING, SEVERE
    UNIQUE(feed_message_id, entity_id)
);

-- Alert active periods
CREATE TABLE IF NOT EXISTS alert_active_periods (
    alert_active_period_id SERIAL PRIMARY KEY,
    alert_id INTEGER NOT NULL REFERENCES alerts(alert_id) ON DELETE CASCADE,
    start_time BIGINT, -- POSIX time, always UTC
    end_time BIGINT -- POSIX time, always UTC
);

-- Alert informed entities (what the alert affects)
CREATE TABLE IF NOT EXISTS alert_informed_entities (
    informed_entity_id SERIAL PRIMARY KEY,
    alert_id INTEGER NOT NULL REFERENCES alerts(alert_id) ON DELETE CASCADE,
    agency_id VARCHAR(50),
    route_id VARCHAR(50),
    direction_id SMALLINT,
    trip_id VARCHAR(100),
    trip_route_id VARCHAR(50),
    trip_start_time INTEGER, -- Seconds since midnight (can exceed 86400 for next-day services)
    trip_start_date DATE,
    stop_id VARCHAR(50)
);

-- Alert translations (for URL and text fields)
CREATE TABLE IF NOT EXISTS alert_translations (
    translation_id SERIAL PRIMARY KEY,
    alert_id INTEGER NOT NULL REFERENCES alerts(alert_id) ON DELETE CASCADE,
    field_type VARCHAR(20) NOT NULL, -- 'url', 'header_text', 'description_text'
    language VARCHAR(10), -- BCP-47 language code
    text TEXT NOT NULL
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_feed_messages_timestamp ON feed_messages(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_feed_messages_source ON feed_messages(source_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_feed_messages_version ON feed_messages(version_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_vehicle_positions_timestamp ON vehicle_positions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_vehicle_positions_trip ON vehicle_positions(trip_id) WHERE trip_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_vehicle_positions_route ON vehicle_positions(route_id) WHERE route_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_vehicle_positions_location ON vehicle_positions USING GIST (ll_to_earth(latitude, longitude));
CREATE INDEX IF NOT EXISTS idx_vehicle_positions_feed ON vehicle_positions(feed_message_id);
CREATE INDEX IF NOT EXISTS idx_vehicle_positions_vehicle ON vehicle_positions(vehicle_id) WHERE vehicle_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_trip_updates_trip ON trip_updates(trip_id);
CREATE INDEX IF NOT EXISTS idx_trip_updates_timestamp ON trip_updates(timestamp DESC) WHERE timestamp IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_trip_updates_feed ON trip_updates(feed_message_id);
CREATE INDEX IF NOT EXISTS idx_trip_updates_route ON trip_updates(route_id) WHERE route_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stop_time_updates_trip ON stop_time_updates(trip_update_id);
CREATE INDEX IF NOT EXISTS idx_stop_time_updates_stop ON stop_time_updates(stop_id) WHERE stop_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_stop_time_updates_arrival ON stop_time_updates(arrival_time) WHERE arrival_time IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_alerts_feed ON alerts(feed_message_id);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);

CREATE INDEX IF NOT EXISTS idx_alert_active_periods_alert ON alert_active_periods(alert_id);
CREATE INDEX IF NOT EXISTS idx_alert_active_periods_time ON alert_active_periods(start_time, end_time);

CREATE INDEX IF NOT EXISTS idx_alert_informed_entities_alert ON alert_informed_entities(alert_id);
CREATE INDEX IF NOT EXISTS idx_alert_informed_entities_route ON alert_informed_entities(route_id) WHERE route_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_alert_informed_entities_stop ON alert_informed_entities(stop_id) WHERE stop_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_alert_informed_entities_trip ON alert_informed_entities(trip_id) WHERE trip_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_alert_translations_alert ON alert_translations(alert_id, field_type);