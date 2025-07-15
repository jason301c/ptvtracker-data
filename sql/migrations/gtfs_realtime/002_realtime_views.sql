-- GTFS Realtime Views
-- Views for joining realtime data with static GTFS data

SET search_path TO gtfs_rt, gtfs, public;

-- Current vehicle positions (latest for each vehicle)
CREATE OR REPLACE VIEW current_vehicle_positions AS
WITH latest_feed AS (
    SELECT source_id, version_id, MAX(timestamp) as max_timestamp
    FROM feed_messages
    WHERE received_at > NOW() - INTERVAL '10 minutes'
      AND version_id IN (SELECT version_id FROM gtfs.versions WHERE is_active = TRUE)
    GROUP BY source_id, version_id
)
SELECT DISTINCT ON (vp.vehicle_id, vp.trip_id)
    vp.*,
    fm.version_id,
    ts.source_name,
    r.route_short_name,
    r.route_long_name,
    s.stop_name as current_stop_name
FROM vehicle_positions vp
JOIN feed_messages fm ON vp.feed_message_id = fm.feed_message_id
JOIN latest_feed lf ON fm.source_id = lf.source_id AND fm.version_id = lf.version_id AND fm.timestamp = lf.max_timestamp
JOIN gtfs.transport_sources ts ON fm.source_id = ts.source_id
LEFT JOIN gtfs.routes r ON vp.route_id = r.route_id AND fm.source_id = r.source_id AND fm.version_id = r.version_id
LEFT JOIN gtfs.stops s ON vp.stop_id = s.stop_id AND fm.source_id = s.source_id AND fm.version_id = s.version_id
ORDER BY vp.vehicle_id, vp.trip_id, vp.timestamp DESC;

-- Current trip delays
CREATE OR REPLACE VIEW current_trip_delays AS
WITH latest_feed AS (
    SELECT source_id, version_id, MAX(timestamp) as max_timestamp
    FROM feed_messages
    WHERE received_at > NOW() - INTERVAL '10 minutes'
      AND version_id IN (SELECT version_id FROM gtfs.versions WHERE is_active = TRUE)
    GROUP BY source_id, version_id
)
SELECT DISTINCT ON (tu.trip_id)
    tu.*,
    fm.version_id,
    ts.source_name,
    r.route_short_name,
    r.route_long_name,
    t.trip_headsign
FROM trip_updates tu
JOIN feed_messages fm ON tu.feed_message_id = fm.feed_message_id
JOIN latest_feed lf ON fm.source_id = lf.source_id AND fm.version_id = lf.version_id AND fm.timestamp = lf.max_timestamp
JOIN gtfs.transport_sources ts ON fm.source_id = ts.source_id
LEFT JOIN gtfs.trips t ON tu.trip_id = t.trip_id AND fm.source_id = t.source_id AND fm.version_id = t.version_id
LEFT JOIN gtfs.routes r ON COALESCE(tu.route_id, t.route_id) = r.route_id AND fm.source_id = r.source_id AND fm.version_id = r.version_id
WHERE tu.schedule_relationship != 3 -- Not canceled
ORDER BY tu.trip_id, tu.timestamp DESC;

-- Active alerts
CREATE OR REPLACE VIEW active_alerts AS
SELECT DISTINCT ON (a.entity_id, fm.source_id)
    a.*,
    fm.version_id,
    ts.source_name,
    fm.timestamp as feed_timestamp,
    fm.received_at
FROM alerts a
JOIN feed_messages fm ON a.feed_message_id = fm.feed_message_id
JOIN gtfs.transport_sources ts ON fm.source_id = ts.source_id
LEFT JOIN alert_active_periods ap ON a.alert_id = ap.alert_id
WHERE (ap.start_time IS NULL OR ap.start_time <= NOW())
  AND (ap.end_time IS NULL OR ap.end_time >= NOW())
  AND fm.received_at > NOW() - INTERVAL '24 hours'
  AND fm.version_id IN (SELECT version_id FROM gtfs.versions WHERE is_active = TRUE)
ORDER BY a.entity_id, fm.source_id, fm.timestamp DESC;

-- Stop arrival predictions
CREATE OR REPLACE VIEW stop_arrival_predictions AS
SELECT 
    stu.stop_id,
    s.stop_name,
    tu.trip_id,
    tu.route_id,
    r.route_short_name,
    r.route_long_name,
    t.trip_headsign,
    stu.arrival_time,
    stu.arrival_delay,
    stu.departure_time,
    stu.departure_delay,
    tu.timestamp as last_update,
    fm.version_id,
    ts.source_name
FROM stop_time_updates stu
JOIN trip_updates tu ON stu.trip_update_id = tu.trip_update_id
JOIN feed_messages fm ON tu.feed_message_id = fm.feed_message_id
JOIN gtfs.transport_sources ts ON fm.source_id = ts.source_id
LEFT JOIN gtfs.stops s ON stu.stop_id = s.stop_id AND fm.source_id = s.source_id AND fm.version_id = s.version_id
LEFT JOIN gtfs.trips t ON tu.trip_id = t.trip_id AND fm.source_id = t.source_id AND fm.version_id = t.version_id
LEFT JOIN gtfs.routes r ON COALESCE(tu.route_id, t.route_id) = r.route_id AND fm.source_id = r.source_id AND fm.version_id = r.version_id
WHERE stu.schedule_relationship = 0 -- Scheduled
  AND (stu.arrival_time IS NULL OR stu.arrival_time > NOW())
  AND fm.received_at > NOW() - INTERVAL '10 minutes'
  AND fm.version_id IN (SELECT version_id FROM gtfs.versions WHERE is_active = TRUE);

-- Vehicle occupancy summary
CREATE OR REPLACE VIEW vehicle_occupancy_summary AS
SELECT 
    vp.route_id,
    r.route_short_name,
    r.route_long_name,
    vp.version_id,
    ts.source_name,
    COUNT(*) as vehicle_count,
    COUNT(*) FILTER (WHERE vp.occupancy_status = 0) as empty_count,
    COUNT(*) FILTER (WHERE vp.occupancy_status IN (1,2)) as seats_available_count,
    COUNT(*) FILTER (WHERE vp.occupancy_status IN (3,4,5)) as crowded_count,
    AVG(vp.occupancy_percentage) FILTER (WHERE vp.occupancy_percentage IS NOT NULL) as avg_occupancy_percentage
FROM current_vehicle_positions vp
JOIN gtfs.transport_sources ts ON vp.source_name = ts.source_name
LEFT JOIN gtfs.routes r ON vp.route_id = r.route_id AND vp.version_id = r.version_id
WHERE vp.occupancy_status IS NOT NULL OR vp.occupancy_percentage IS NOT NULL
GROUP BY vp.route_id, r.route_short_name, r.route_long_name, vp.version_id, ts.source_name;

-- Realtime data freshness monitoring
CREATE OR REPLACE VIEW realtime_feed_status AS
WITH active_version AS (
    SELECT version_id FROM gtfs.versions WHERE is_active = TRUE LIMIT 1
)
SELECT 
    ts.source_id,
    ts.source_name,
    av.version_id,
    MAX(fm.timestamp) as latest_feed_timestamp,
    MAX(fm.received_at) as latest_received_at,
    COUNT(DISTINCT fm.feed_message_id) FILTER (WHERE fm.received_at > NOW() - INTERVAL '1 hour') as messages_last_hour,
    COUNT(DISTINCT vp.vehicle_id) FILTER (WHERE fm.received_at > NOW() - INTERVAL '10 minutes') as active_vehicles,
    COUNT(DISTINCT tu.trip_id) FILTER (WHERE fm.received_at > NOW() - INTERVAL '10 minutes') as active_trip_updates,
    COUNT(DISTINCT a.alert_id) FILTER (WHERE fm.received_at > NOW() - INTERVAL '24 hours') as active_alerts
FROM gtfs.transport_sources ts
CROSS JOIN active_version av
LEFT JOIN feed_messages fm ON ts.source_id = fm.source_id AND fm.version_id = av.version_id
LEFT JOIN vehicle_positions vp ON fm.feed_message_id = vp.feed_message_id
LEFT JOIN trip_updates tu ON fm.feed_message_id = tu.feed_message_id
LEFT JOIN alerts a ON fm.feed_message_id = a.feed_message_id
GROUP BY ts.source_id, ts.source_name, av.version_id
ORDER BY ts.source_name;