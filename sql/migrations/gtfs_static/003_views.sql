-- GTFS Static Views for Common Queries
-- Views that simplify access to active version data

SET search_path TO gtfs, public;

-- View for current active version
CREATE OR REPLACE VIEW active_version AS
SELECT version_id, version_name, created_at, updated_at, source_url, description
FROM versions
WHERE is_active = TRUE;

-- Helper views for common queries using active version
CREATE OR REPLACE VIEW active_services AS
SELECT DISTINCT s.service_id, s.source_id, ts.source_name
FROM calendar s
JOIN transport_sources ts ON s.source_id = ts.source_id
JOIN active_version av ON s.version_id = av.version_id
WHERE s.end_date >= CURRENT_DATE 
  AND s.start_date <= CURRENT_DATE;

CREATE OR REPLACE VIEW active_stops AS
SELECT s.*, ts.source_name
FROM stops s
JOIN transport_sources ts ON s.source_id = ts.source_id
JOIN active_version av ON s.version_id = av.version_id;

CREATE OR REPLACE VIEW active_routes AS
SELECT r.*, ts.source_name
FROM routes r
JOIN transport_sources ts ON r.source_id = ts.source_id
JOIN active_version av ON r.version_id = av.version_id;

CREATE OR REPLACE VIEW active_trips AS
SELECT t.*, ts.source_name
FROM trips t
JOIN transport_sources ts ON t.source_id = ts.source_id
JOIN active_version av ON t.version_id = av.version_id;

CREATE OR REPLACE VIEW active_stop_times AS
SELECT st.*, ts.source_name
FROM stop_times st
JOIN transport_sources ts ON st.source_id = ts.source_id
JOIN active_version av ON st.version_id = av.version_id;

-- Route summary view for active version
CREATE OR REPLACE VIEW route_summary AS
SELECT 
    r.route_id,
    r.source_id,
    ts.source_name,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    COUNT(DISTINCT t.trip_id) as trip_count,
    COUNT(DISTINCT st.stop_id) as stop_count
FROM routes r
JOIN transport_sources ts ON r.source_id = ts.source_id
JOIN active_version av ON r.version_id = av.version_id
LEFT JOIN trips t ON r.route_id = t.route_id AND r.source_id = t.source_id AND r.version_id = t.version_id
LEFT JOIN stop_times st ON t.trip_id = st.trip_id AND t.source_id = st.source_id AND t.version_id = st.version_id
GROUP BY r.route_id, r.source_id, ts.source_name, r.route_short_name, r.route_long_name, r.route_type;

-- Version comparison view
CREATE OR REPLACE VIEW version_stats AS
SELECT 
    v.version_id,
    v.version_name,
    v.created_at,
    v.is_active,
    COUNT(DISTINCT s.stop_id) as stop_count,
    COUNT(DISTINCT r.route_id) as route_count,
    COUNT(DISTINCT t.trip_id) as trip_count,
    COUNT(DISTINCT st.trip_id || '-' || st.stop_sequence) as stop_time_count
FROM versions v
LEFT JOIN stops s ON v.version_id = s.version_id
LEFT JOIN routes r ON v.version_id = r.version_id
LEFT JOIN trips t ON v.version_id = t.version_id
LEFT JOIN stop_times st ON v.version_id = st.version_id
GROUP BY v.version_id, v.version_name, v.created_at, v.is_active
ORDER BY v.created_at DESC;

-- Stop connections view for active version
CREATE OR REPLACE VIEW stop_connections AS
SELECT DISTINCT
    st1.stop_id as from_stop_id,
    s1.stop_name as from_stop_name,
    st2.stop_id as to_stop_id,
    s2.stop_name as to_stop_name,
    r.route_short_name,
    r.route_long_name,
    ts.source_name
FROM stop_times st1
JOIN stop_times st2 ON st1.trip_id = st2.trip_id 
    AND st1.source_id = st2.source_id 
    AND st1.version_id = st2.version_id
    AND st1.stop_sequence = st2.stop_sequence - 1
JOIN stops s1 ON st1.stop_id = s1.stop_id 
    AND st1.source_id = s1.source_id 
    AND st1.version_id = s1.version_id
JOIN stops s2 ON st2.stop_id = s2.stop_id 
    AND st2.source_id = s2.source_id 
    AND st2.version_id = s2.version_id
JOIN trips t ON st1.trip_id = t.trip_id 
    AND st1.source_id = t.source_id 
    AND st1.version_id = t.version_id
JOIN routes r ON t.route_id = r.route_id 
    AND t.source_id = r.source_id 
    AND t.version_id = r.version_id
JOIN transport_sources ts ON r.source_id = ts.source_id
JOIN active_version av ON st1.version_id = av.version_id;

-- Service calendar overview for active version
CREATE OR REPLACE VIEW service_calendar_overview AS
SELECT 
    c.service_id,
    ts.source_name,
    c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday, c.sunday,
    c.start_date,
    c.end_date,
    COUNT(DISTINCT cd.date) FILTER (WHERE cd.exception_type = 1) as added_dates,
    COUNT(DISTINCT cd.date) FILTER (WHERE cd.exception_type = 2) as removed_dates
FROM calendar c
JOIN transport_sources ts ON c.source_id = ts.source_id
JOIN active_version av ON c.version_id = av.version_id
LEFT JOIN calendar_dates cd ON c.service_id = cd.service_id 
    AND c.source_id = cd.source_id 
    AND c.version_id = cd.version_id
GROUP BY c.service_id, ts.source_name, c.monday, c.tuesday, c.wednesday, 
         c.thursday, c.friday, c.saturday, c.sunday, c.start_date, c.end_date;