-- Migration: Improved GTFS-RT Notification System
-- Description: Optimizes notification system for production-grade performance
-- Author: Claude
-- Date: 2025-01-21

-- Drop existing functions to recreate with improvements
DROP FUNCTION IF EXISTS gtfs_rt.notify_batch_update_efficient;
DROP FUNCTION IF EXISTS gtfs_rt.notify_batch_update;

-- Create improved notification function that's called after bulk inserts
CREATE OR REPLACE FUNCTION gtfs_rt.notify_stop_updates(
    p_source_id INTEGER,
    p_stop_ids TEXT[]
) RETURNS VOID AS $$
DECLARE
    v_stop_id TEXT;
    v_channel TEXT;
    v_payload TEXT;
BEGIN
    -- Send individual notifications for each affected stop
    FOREACH v_stop_id IN ARRAY p_stop_ids
    LOOP
        v_channel := format('stop_departures:%s:%s', p_source_id, v_stop_id);
        v_payload := json_build_object(
            'type', 'update',
            'source_id', p_source_id,
            'stop_id', v_stop_id,
            'timestamp', extract(epoch from now())
        )::text;
        
        PERFORM pg_notify(v_channel, v_payload);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Optimized function to get real-time departures for a stop
-- This is called immediately when a client connects to SSE
CREATE OR REPLACE FUNCTION gtfs_rt.get_stop_realtime_departures_optimized(
    p_source_id INTEGER,
    p_stop_id TEXT,
    p_limit INTEGER DEFAULT 20
) RETURNS TABLE (
    trip_id TEXT,
    route_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_color TEXT,
    route_text_color TEXT,
    trip_headsign TEXT,
    direction_id INTEGER,
    scheduled_departure_time TIMESTAMPTZ,
    realtime_departure_time TIMESTAMPTZ,
    departure_delay INTEGER,
    arrival_delay INTEGER,
    schedule_relationship INTEGER,
    vehicle_id TEXT,
    vehicle_label TEXT,
    last_updated TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    WITH current_time_info AS (
        SELECT 
            CURRENT_DATE as today,
            CURRENT_TIME as now_time,
            CASE 
                WHEN CURRENT_TIME < '03:00:00'::time THEN CURRENT_DATE - INTERVAL '1 day'
                ELSE CURRENT_DATE
            END as service_date
    ),
    realtime_updates AS (
        SELECT DISTINCT ON (stu.stop_id, tu.trip_id)
            tu.trip_id,
            tu.route_id,
            tu.direction_id,
            tu.vehicle_id,
            tu.vehicle_label,
            tu.timestamp as last_updated,
            stu.arrival_delay,
            stu.departure_delay,
            stu.arrival_time,
            stu.departure_time,
            stu.schedule_relationship,
            fm.received_at
        FROM gtfs_rt.stop_time_updates stu
        JOIN gtfs_rt.trip_updates tu ON stu.trip_update_id = tu.trip_update_id
        JOIN gtfs_rt.feed_messages fm ON tu.feed_message_id = fm.feed_message_id
        WHERE stu.stop_id = p_stop_id
          AND fm.source_id = p_source_id
          AND fm.received_at > NOW() - INTERVAL '2 hours'
        ORDER BY stu.stop_id, tu.trip_id, fm.received_at DESC
    )
    SELECT 
        st.trip_id,
        t.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_color,
        r.route_text_color,
        t.trip_headsign,
        t.direction_id,
        -- Scheduled departure time
        (cti.service_date + st.departure_time::interval) as scheduled_departure_time,
        -- Real-time departure time
        CASE 
            WHEN ru.departure_time IS NOT NULL THEN 
                TIMESTAMP WITH TIME ZONE 'epoch' + ru.departure_time * INTERVAL '1 second'
            WHEN ru.departure_delay IS NOT NULL THEN 
                (cti.service_date + st.departure_time::interval) + (ru.departure_delay || ' seconds')::interval
            ELSE 
                (cti.service_date + st.departure_time::interval)
        END as realtime_departure_time,
        COALESCE(ru.departure_delay, 0) as departure_delay,
        COALESCE(ru.arrival_delay, 0) as arrival_delay,
        COALESCE(ru.schedule_relationship, 0) as schedule_relationship,
        ru.vehicle_id,
        ru.vehicle_label,
        ru.last_updated
    FROM current_time_info cti
    CROSS JOIN gtfs.stop_times st
    JOIN gtfs.trips t ON st.trip_id = t.trip_id
    JOIN gtfs.routes r ON t.route_id = r.route_id
    LEFT JOIN realtime_updates ru ON st.trip_id = ru.trip_id
    WHERE st.stop_id = p_stop_id
      AND t.source_id = p_source_id
      AND t.version_id = (SELECT version_id FROM gtfs.versions WHERE is_active = TRUE LIMIT 1)
      -- Only show departures in the next 2 hours
      AND (cti.service_date + st.departure_time::interval) > NOW()
      AND (cti.service_date + st.departure_time::interval) < NOW() + INTERVAL '2 hours'
    ORDER BY 
        CASE 
            WHEN ru.departure_time IS NOT NULL THEN 
                TIMESTAMP WITH TIME ZONE 'epoch' + ru.departure_time * INTERVAL '1 second'
            WHEN ru.departure_delay IS NOT NULL THEN 
                (cti.service_date + st.departure_time::interval) + (ru.departure_delay || ' seconds')::interval
            ELSE 
                (cti.service_date + st.departure_time::interval)
        END
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql STABLE;

-- Create indexes to support fast real-time queries
CREATE INDEX IF NOT EXISTS idx_stop_time_updates_stop_id_lookup 
ON gtfs_rt.stop_time_updates(stop_id);

CREATE INDEX IF NOT EXISTS idx_trip_updates_timestamp 
ON gtfs_rt.trip_updates(feed_message_id, trip_id);

CREATE INDEX IF NOT EXISTS idx_feed_messages_recent 
ON gtfs_rt.feed_messages(source_id, received_at DESC)
WHERE received_at > NOW() - INTERVAL '2 hours';

-- Simplified trigger for stop_time_updates notifications
CREATE OR REPLACE FUNCTION gtfs_rt.notify_stop_time_update() RETURNS TRIGGER AS $$
DECLARE
    v_source_id INTEGER;
    v_channel TEXT;
    v_payload TEXT;
BEGIN
    -- Get source_id from the related tables
    SELECT fm.source_id INTO v_source_id
    FROM gtfs_rt.trip_updates tu
    JOIN gtfs_rt.feed_messages fm ON tu.feed_message_id = fm.feed_message_id
    WHERE tu.trip_update_id = NEW.trip_update_id;
    
    -- Send notification for this specific stop
    v_channel := format('stop_departures:%s:%s', v_source_id, NEW.stop_id);
    v_payload := json_build_object(
        'type', 'update',
        'source_id', v_source_id,
        'stop_id', NEW.stop_id,
        'trip_update_id', NEW.trip_update_id,
        'timestamp', extract(epoch from now())
    )::text;
    
    PERFORM pg_notify(v_channel, v_payload);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop and recreate the trigger to ensure it's properly configured
DROP TRIGGER IF EXISTS stop_time_updates_notify_trigger ON gtfs_rt.stop_time_updates;
CREATE TRIGGER stop_time_updates_notify_trigger
    AFTER INSERT OR UPDATE ON gtfs_rt.stop_time_updates
    FOR EACH ROW
    EXECUTE FUNCTION gtfs_rt.notify_stop_time_update();

-- Add comment explaining the notification system
COMMENT ON FUNCTION gtfs_rt.notify_stop_updates IS 
'Production-grade notification function called by Go processor after bulk inserts. 
Sends individual notifications for each affected stop to minimize frontend processing.';

COMMENT ON FUNCTION gtfs_rt.get_stop_realtime_departures_optimized IS 
'Optimized function to fetch real-time departures for SSE connections. 
Returns combined static schedule and real-time updates for the next 2 hours.';