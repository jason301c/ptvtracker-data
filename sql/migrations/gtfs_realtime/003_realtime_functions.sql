-- GTFS Realtime Functions
-- Functions for querying and managing realtime data

SET search_path TO gtfs_rt, gtfs, public;

-- Function to get next arrivals at a stop
CREATE OR REPLACE FUNCTION get_next_arrivals_at_stop(
    p_stop_id VARCHAR(50),
    p_source_id INTEGER DEFAULT NULL,
    p_limit INTEGER DEFAULT 10,
    p_version_id INTEGER DEFAULT NULL
)
RETURNS TABLE (
    trip_id VARCHAR(100),
    route_id VARCHAR(50),
    route_short_name VARCHAR(50),
    route_long_name VARCHAR(255),
    trip_headsign VARCHAR(255),
    scheduled_arrival TIME,
    predicted_arrival TIMESTAMP,
    delay_seconds INTEGER,
    vehicle_id VARCHAR(100),
    occupancy_status SMALLINT,
    last_update TIMESTAMP,
    version_id INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH active_version AS (
        SELECT COALESCE(p_version_id, (SELECT v.version_id FROM gtfs.versions v WHERE v.is_active = TRUE LIMIT 1)) as vid
    ),
    static_schedule AS (
        -- Get scheduled arrivals from static GTFS
        SELECT 
            st.trip_id,
            t.route_id,
            r.route_short_name,
            r.route_long_name,
            t.trip_headsign,
            st.arrival_time,
            st.source_id,
            st.version_id,
            t.service_id
        FROM gtfs.stop_times st
        JOIN gtfs.trips t ON st.trip_id = t.trip_id AND st.source_id = t.source_id AND st.version_id = t.version_id
        JOIN gtfs.routes r ON t.route_id = r.route_id AND t.source_id = r.source_id AND t.version_id = r.version_id
        JOIN gtfs.calendar s ON t.service_id = s.service_id AND t.source_id = s.source_id AND t.version_id = s.version_id
        JOIN active_version av ON st.version_id = av.vid
        WHERE st.stop_id = p_stop_id
          AND (p_source_id IS NULL OR st.source_id = p_source_id)
          AND st.arrival_time >= LOCALTIME - INTERVAL '5 minutes'
          AND st.arrival_time <= LOCALTIME + INTERVAL '2 hours'
          AND s.end_date >= CURRENT_DATE AND s.start_date <= CURRENT_DATE
    ),
    realtime_updates AS (
        -- Get realtime updates
        SELECT 
            tu.trip_id,
            stu.arrival_delay,
            stu.arrival_time as predicted_arrival,
            vp.vehicle_id,
            vp.occupancy_status,
            GREATEST(tu.timestamp, vp.timestamp) as last_update,
            fm.version_id
        FROM trip_updates tu
        JOIN stop_time_updates stu ON tu.trip_update_id = stu.trip_update_id
        JOIN feed_messages fm ON tu.feed_message_id = fm.feed_message_id
        JOIN active_version av ON fm.version_id = av.vid
        LEFT JOIN vehicle_positions vp ON tu.trip_id = vp.trip_id 
            AND tu.feed_message_id = vp.feed_message_id
        WHERE stu.stop_id = p_stop_id
          AND tu.schedule_relationship = 0 -- Scheduled trips only
          AND fm.received_at > NOW() - INTERVAL '10 minutes'
    )
    SELECT 
        ss.trip_id,
        ss.route_id,
        ss.route_short_name,
        ss.route_long_name,
        ss.trip_headsign,
        ss.arrival_time as scheduled_arrival,
        COALESCE(
            ru.predicted_arrival,
            CURRENT_DATE + ss.arrival_time + (ru.arrival_delay || ' seconds')::INTERVAL
        ) as predicted_arrival,
        ru.arrival_delay as delay_seconds,
        ru.vehicle_id,
        ru.occupancy_status,
        ru.last_update,
        ss.version_id
    FROM static_schedule ss
    LEFT JOIN realtime_updates ru ON ss.trip_id = ru.trip_id AND ss.version_id = ru.version_id
    ORDER BY COALESCE(ru.predicted_arrival, CURRENT_DATE + ss.arrival_time)
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- Function to track a specific vehicle
CREATE OR REPLACE FUNCTION track_vehicle(
    p_vehicle_id VARCHAR(100),
    p_source_id INTEGER DEFAULT NULL,
    p_version_id INTEGER DEFAULT NULL
)
RETURNS TABLE (
    timestamp TIMESTAMP,
    trip_id VARCHAR(100),
    route_id VARCHAR(50),
    route_name VARCHAR(255),
    latitude NUMERIC(10,7),
    longitude NUMERIC(10,7),
    speed NUMERIC(5,2),
    bearing NUMERIC(5,2),
    current_stop_id VARCHAR(50),
    current_stop_name VARCHAR(255),
    occupancy_status SMALLINT,
    delay_seconds INTEGER,
    version_id INTEGER
) AS $$
BEGIN
    RETURN QUERY
    WITH active_version AS (
        SELECT COALESCE(p_version_id, (SELECT v.version_id FROM gtfs.versions v WHERE v.is_active = TRUE LIMIT 1)) as vid
    )
    SELECT 
        vp.timestamp,
        vp.trip_id,
        vp.route_id,
        COALESCE(r.route_short_name, r.route_long_name) as route_name,
        vp.latitude,
        vp.longitude,
        vp.speed,
        vp.bearing,
        vp.stop_id as current_stop_id,
        s.stop_name as current_stop_name,
        vp.occupancy_status,
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
    speed NUMERIC(5,2),
    occupancy_status SMALLINT,
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
        vp.speed,
        vp.occupancy_status,
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
    avg_occupancy_percentage NUMERIC,
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
            AVG(vp.occupancy_percentage) as trip_occupancy,
            fm.version_id
        FROM trip_updates tu
        JOIN feed_messages fm ON tu.feed_message_id = fm.feed_message_id
        JOIN active_version av ON fm.version_id = av.vid
        LEFT JOIN vehicle_positions vp ON tu.trip_id = vp.trip_id 
            AND tu.feed_message_id = vp.feed_message_id
        WHERE tu.route_id = p_route_id
          AND fm.source_id = p_source_id
          AND fm.received_at > NOW() - p_time_window
        GROUP BY tu.trip_id, tu.delay, tu.schedule_relationship, fm.version_id
    )
    SELECT 
        AVG(delay)::NUMERIC as avg_delay_seconds,
        MAX(delay) as max_delay_seconds,
        (COUNT(*) FILTER (WHERE ABS(delay) <= 300) * 100.0 / NULLIF(COUNT(*), 0))::NUMERIC as on_time_percentage,
        COUNT(*)::INTEGER as total_trips,
        COUNT(*) FILTER (WHERE delay > 300)::INTEGER as delayed_trips,
        COUNT(*) FILTER (WHERE schedule_relationship = 3)::INTEGER as canceled_trips,
        AVG(trip_occupancy)::NUMERIC as avg_occupancy_percentage,
        MAX(version_id)::INTEGER as version_id
    FROM trip_delays;
END;
$$ LANGUAGE plpgsql;