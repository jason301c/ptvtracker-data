-- GTFS Realtime Functions
-- Functions for querying and managing realtime data

SET search_path TO gtfs_rt, gtfs, public;

-- Function to track a specific vehicle
CREATE OR REPLACE FUNCTION track_vehicle(
    p_vehicle_id VARCHAR(100),
    p_source_id INTEGER DEFAULT NULL,
    p_version_id INTEGER DEFAULT NULL
)
RETURNS TABLE (
    event_timestamp TIMESTAMP,
    trip_id VARCHAR(100),
    route_id VARCHAR(50),
    route_name VARCHAR(255),
    latitude NUMERIC(10,7),
    longitude NUMERIC(10,7),
    bearing NUMERIC(5,2),
    current_stop_id VARCHAR(50),
    current_stop_name VARCHAR(255),
    current_status SMALLINT,
    delay_seconds INTEGER,
    version_id INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH active_version AS (
        SELECT COALESCE(p_version_id, (SELECT v.version_id FROM gtfs.versions v WHERE v.is_active = TRUE LIMIT 1)) as vid
    )
    SELECT 
        vp.timestamp as event_timestamp,
        vp.trip_id,
        vp.route_id,
        COALESCE(r.route_short_name, r.route_long_name) as route_name,
        vp.latitude,
        vp.longitude,
        vp.bearing,
        vp.stop_id as current_stop_id,
        s.stop_name as current_stop_name,
        vp.current_status,
        tu.delay as delay_seconds,
        fm.version_id
    FROM vehicle_positions vp
    JOIN feed_messages fm ON vp.feed_message_id = fm.feed_message_id
    JOIN active_version av ON fm.version_id = av.vid
    LEFT JOIN gtfs.routes r ON vp.route_id = r.route_id AND fm.source_id = r.source_id AND fm.version_id = r.version_id
    LEFT JOIN gtfs.stops s ON vp.stop_id = s.stop_id AND fm.source_id = s.source_id AND fm.version_id = s.version_id
    LEFT JOIN trip_updates tu ON vp.trip_id = tu.trip_id AND vp.feed_message_id = tu.feed_message_id
    WHERE vp.vehicle_id = p_vehicle_id
      AND (p_source_id IS NULL OR fm.source_id = p_source_id)
      AND fm.received_at > NOW() - INTERVAL '1 hour'
    ORDER BY vp.timestamp DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to get vehicles near a location
CREATE OR REPLACE FUNCTION vehicles_near_location(
    lat NUMERIC,
    lon NUMERIC,
    radius_meters INTEGER DEFAULT 1000,
    p_route_type SMALLINT DEFAULT NULL,
    p_version_id INTEGER DEFAULT NULL
)
RETURNS TABLE (
    vehicle_id VARCHAR(100),
    trip_id VARCHAR(100),
    route_id VARCHAR(50),
    route_short_name VARCHAR(50),
    route_long_name VARCHAR(255),
    route_type SMALLINT,
    latitude NUMERIC(10,7),
    longitude NUMERIC(10,7),
    distance_meters NUMERIC,
    bearing NUMERIC(5,2),
    current_status SMALLINT,
    last_update TIMESTAMP,
    version_id INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH active_version AS (
        SELECT COALESCE(p_version_id, (SELECT v.version_id FROM gtfs.versions v WHERE v.is_active = TRUE LIMIT 1)) as vid
    )
    SELECT 
        vp.vehicle_id,
        vp.trip_id,
        vp.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        vp.latitude,
        vp.longitude,
        earth_distance(ll_to_earth(vp.latitude, vp.longitude), ll_to_earth(lat, lon))::NUMERIC as distance_meters,
        vp.bearing,
        vp.current_status,
        vp.timestamp as last_update,
        vp.version_id
    FROM current_vehicle_positions vp
    JOIN active_version av ON vp.version_id = av.vid
    LEFT JOIN gtfs.routes r ON vp.route_id = r.route_id AND vp.version_id = r.version_id
    WHERE earth_box(ll_to_earth(lat, lon), radius_meters) @> ll_to_earth(vp.latitude, vp.longitude)
      AND (p_route_type IS NULL OR r.route_type = p_route_type)
    ORDER BY distance_meters;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up old realtime data
CREATE OR REPLACE FUNCTION cleanup_old_realtime_data(
    retention_hours INTEGER DEFAULT 24
)
RETURNS TABLE (
    deleted_feed_messages INTEGER,
    deleted_vehicle_positions INTEGER,
    deleted_trip_updates INTEGER,
    deleted_alerts INTEGER
) AS $$
DECLARE
    cutoff_time TIMESTAMP;
    del_feeds INTEGER;
    del_vehicles INTEGER;
    del_trips INTEGER;
    del_alerts INTEGER;
BEGIN
    cutoff_time := NOW() - (retention_hours || ' hours')::INTERVAL;
    
    -- Delete old feed messages (cascades to all child tables)
    DELETE FROM feed_messages WHERE received_at < cutoff_time;
    GET DIAGNOSTICS del_feeds = ROW_COUNT;
    
    -- Count deletions from child tables (for reporting)
    SELECT COUNT(*) INTO del_vehicles FROM vehicle_positions vp
    JOIN feed_messages fm ON vp.feed_message_id = fm.feed_message_id
    WHERE fm.received_at < cutoff_time;
    
    SELECT COUNT(*) INTO del_trips FROM trip_updates tu
    JOIN feed_messages fm ON tu.feed_message_id = fm.feed_message_id
    WHERE fm.received_at < cutoff_time;
    
    SELECT COUNT(*) INTO del_alerts FROM alerts a
    JOIN feed_messages fm ON a.feed_message_id = fm.feed_message_id
    WHERE fm.received_at < cutoff_time;
    
    RETURN QUERY SELECT del_feeds, del_vehicles, del_trips, del_alerts;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate route performance metrics
CREATE OR REPLACE FUNCTION calculate_route_performance(
    p_route_id VARCHAR(50),
    p_source_id INTEGER,
    p_time_window INTERVAL DEFAULT '1 hour',
    p_version_id INTEGER DEFAULT NULL
)
RETURNS TABLE (
    avg_delay_seconds NUMERIC,
    max_delay_seconds INTEGER,
    on_time_percentage NUMERIC,
    total_trips INTEGER,
    delayed_trips INTEGER,
    canceled_trips INTEGER,
    version_id INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH active_version AS (
        SELECT COALESCE(p_version_id, (SELECT v.version_id FROM gtfs.versions v WHERE v.is_active = TRUE LIMIT 1)) as vid
    ),
    trip_delays AS (
        SELECT 
            tu.trip_id,
            tu.delay,
            tu.schedule_relationship,
            fm.version_id
        FROM trip_updates tu
        JOIN feed_messages fm ON tu.feed_message_id = fm.feed_message_id
        JOIN active_version av ON fm.version_id = av.vid
        WHERE tu.route_id = p_route_id
          AND fm.source_id = p_source_id
          AND fm.received_at > NOW() - p_time_window
          AND tu.is_deleted = FALSE
    )
    SELECT 
        AVG(delay)::NUMERIC as avg_delay_seconds,
        MAX(delay) as max_delay_seconds,
        (COUNT(*) FILTER (WHERE ABS(delay) <= 300) * 100.0 / NULLIF(COUNT(*), 0))::NUMERIC as on_time_percentage,
        COUNT(*)::INTEGER as total_trips,
        COUNT(*) FILTER (WHERE delay > 300)::INTEGER as delayed_trips,
        COUNT(*) FILTER (WHERE schedule_relationship = 3)::INTEGER as canceled_trips,
        MAX(version_id)::INTEGER as version_id
    FROM trip_delays;
END;
$$ LANGUAGE plpgsql;

-- Function to get active alerts for a route or stop
CREATE OR REPLACE FUNCTION get_active_alerts(
    p_route_id VARCHAR(50) DEFAULT NULL,
    p_stop_id VARCHAR(50) DEFAULT NULL,
    p_source_id INTEGER DEFAULT NULL,
    p_version_id INTEGER DEFAULT NULL
)
RETURNS TABLE (
    alert_id INTEGER,
    entity_id VARCHAR(100),
    cause SMALLINT,
    effect SMALLINT,
    severity SMALLINT,
    url TEXT,
    header_text TEXT,
    description_text TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    affected_routes TEXT[],
    affected_stops TEXT[],
    source_name VARCHAR(100),
    version_id INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH active_version AS (
        SELECT COALESCE(p_version_id, (SELECT v.version_id FROM gtfs.versions v WHERE v.is_active = TRUE LIMIT 1)) as vid
    )
    SELECT 
        a.alert_id,
        a.entity_id,
        a.cause,
        a.effect,
        a.severity,
        url_trans.text as url,
        header_trans.text as header_text,
        desc_trans.text as description_text,
        CASE WHEN ap.start_time IS NOT NULL THEN TO_TIMESTAMP(ap.start_time) ELSE NULL END as start_time,
        CASE WHEN ap.end_time IS NOT NULL THEN TO_TIMESTAMP(ap.end_time) ELSE NULL END as end_time,
        ARRAY_AGG(DISTINCT aie.route_id) FILTER (WHERE aie.route_id IS NOT NULL) as affected_routes,
        ARRAY_AGG(DISTINCT aie.stop_id) FILTER (WHERE aie.stop_id IS NOT NULL) as affected_stops,
        ts.source_name,
        fm.version_id
    FROM alerts a
    JOIN feed_messages fm ON a.feed_message_id = fm.feed_message_id
    JOIN active_version av ON fm.version_id = av.vid
    JOIN gtfs.transport_sources ts ON fm.source_id = ts.source_id
    LEFT JOIN alert_active_periods ap ON a.alert_id = ap.alert_id
    LEFT JOIN alert_informed_entities aie ON a.alert_id = aie.alert_id
    LEFT JOIN alert_translations url_trans ON a.alert_id = url_trans.alert_id AND url_trans.field_type = 'url'
    LEFT JOIN alert_translations header_trans ON a.alert_id = header_trans.alert_id AND header_trans.field_type = 'header_text'
    LEFT JOIN alert_translations desc_trans ON a.alert_id = desc_trans.alert_id AND desc_trans.field_type = 'description_text'
    WHERE (p_source_id IS NULL OR fm.source_id = p_source_id)
      AND (p_route_id IS NULL OR aie.route_id = p_route_id)
      AND (p_stop_id IS NULL OR aie.stop_id = p_stop_id)
      AND (ap.start_time IS NULL OR ap.start_time <= EXTRACT(EPOCH FROM NOW()))
      AND (ap.end_time IS NULL OR ap.end_time >= EXTRACT(EPOCH FROM NOW()))
      AND fm.received_at > NOW() - INTERVAL '24 hours'
      AND a.is_deleted = FALSE
    GROUP BY a.alert_id, a.entity_id, a.cause, a.effect, a.severity, 
             url_trans.text, header_trans.text, desc_trans.text, 
             ap.start_time, ap.end_time, ts.source_name, fm.version_id;
END;
$$ LANGUAGE plpgsql;