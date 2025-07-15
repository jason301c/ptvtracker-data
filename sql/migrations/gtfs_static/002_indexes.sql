-- GTFS Static Indexes for Performance Optimization
-- Indexes for spatial, temporal, and text search queries

SET search_path TO gtfs, public;

-- Enable extensions for search functionality
CREATE EXTENSION IF NOT EXISTS pg_trgm; -- For fuzzy text matching
CREATE EXTENSION IF NOT EXISTS unaccent; -- For accent-insensitive search
CREATE EXTENSION IF NOT EXISTS earthdistance CASCADE; -- For ll_to_earth function

-- Version-aware performance indexes
CREATE INDEX idx_versions_active ON versions(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_versions_created ON versions(created_at DESC);

-- Stops location index (requires earthdistance extension)
CREATE INDEX idx_stops_location ON stops 
USING GIST (ll_to_earth(stop_lat, stop_lon)) 
WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL;

-- Stop times indexes (critical for performance)
CREATE INDEX idx_stop_times_arrival ON stop_times(version_id, arrival_time);
CREATE INDEX idx_stop_times_departure ON stop_times(version_id, departure_time);
CREATE INDEX idx_stop_times_stop_id ON stop_times(stop_id, source_id, version_id);
CREATE INDEX idx_stop_times_trip_stop ON stop_times(trip_id, source_id, version_id, stop_sequence);

-- Trips indexes
CREATE INDEX idx_trips_route_service ON trips(route_id, service_id, source_id, version_id);
CREATE INDEX idx_trips_version ON trips(version_id);

-- Calendar and calendar dates indexes
CREATE INDEX idx_calendar_dates_date ON calendar_dates(version_id, date);
CREATE INDEX idx_calendar_service_dates ON calendar(service_id, version_id, start_date, end_date);
CREATE INDEX idx_calendar_version ON calendar(version_id);

-- Shapes index
CREATE INDEX idx_shapes_sequence ON shapes(shape_id, source_id, version_id, shape_pt_sequence);

-- Routes indexes
CREATE INDEX idx_routes_type ON routes(route_type, source_id, version_id);
CREATE INDEX idx_routes_version ON routes(version_id);

-- Transfers indexes
CREATE INDEX idx_transfers_from_stop ON transfers(from_stop_id, source_id, version_id);
CREATE INDEX idx_transfers_to_stop ON transfers(to_stop_id, source_id, version_id);

-- Search optimization indexes
CREATE INDEX idx_stops_name_text ON stops 
USING GIN (to_tsvector('english', stop_name));

CREATE INDEX idx_stops_name_trigram ON stops 
USING GIN (stop_name gin_trgm_ops);

CREATE INDEX idx_routes_name_text ON routes 
USING GIN (to_tsvector('english', coalesce(route_short_name, '') || ' ' || coalesce(route_long_name, '')));

CREATE INDEX idx_routes_short_name ON routes(route_short_name, version_id) 
WHERE route_short_name IS NOT NULL;

CREATE INDEX idx_stops_name_lower ON stops(lower(stop_name), version_id);
CREATE INDEX idx_routes_long_name_lower ON routes(lower(route_long_name), version_id);

-- Composite indexes for common queries
CREATE INDEX idx_stops_version_source ON stops(version_id, source_id);
CREATE INDEX idx_routes_version_source ON routes(version_id, source_id);
CREATE INDEX idx_trips_version_route ON trips(version_id, route_id);

-- Spatial index with PostGIS (uncomment if PostGIS is available)
-- CREATE EXTENSION IF NOT EXISTS postgis;
-- CREATE INDEX idx_stops_geom ON stops USING GIST (ST_Point(stop_lon, stop_lat));

CREATE INDEX idx_stops_version_id ON stops(version_id, stop_id, source_id);
CREATE INDEX idx_routes_version_id ON routes(version_id, route_id, source_id);