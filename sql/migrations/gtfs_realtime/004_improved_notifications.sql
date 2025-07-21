-- Migration: Improved GTFS-RT Performance Indexes
-- Description: Adds indexes to optimize real-time data queries
-- Author: Claude
-- Date: 2025-01-21

-- Drop functions that were replaced by direct implementation
DROP FUNCTION IF EXISTS gtfs_rt.notify_batch_update_efficient;
DROP FUNCTION IF EXISTS gtfs_rt.notify_batch_update;

-- Create indexes to support fast real-time queries
CREATE INDEX IF NOT EXISTS idx_stop_time_updates_stop_id_lookup 
ON gtfs_rt.stop_time_updates(stop_id);

CREATE INDEX IF NOT EXISTS idx_trip_updates_timestamp 
ON gtfs_rt.trip_updates(feed_message_id, trip_id);

CREATE INDEX IF NOT EXISTS idx_feed_messages_recent 
ON gtfs_rt.feed_messages(source_id, received_at DESC)
WHERE received_at > NOW() - INTERVAL '2 hours';

-- Ensure stop_time_updates trigger exists for row-level notifications
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

-- Ensure trigger is properly configured
DROP TRIGGER IF EXISTS stop_time_updates_notify_trigger ON gtfs_rt.stop_time_updates;
CREATE TRIGGER stop_time_updates_notify_trigger
    AFTER INSERT OR UPDATE ON gtfs_rt.stop_time_updates
    FOR EACH ROW
    EXECUTE FUNCTION gtfs_rt.notify_stop_time_update();

COMMENT ON INDEX idx_stop_time_updates_stop_id_lookup IS 
'Speeds up real-time queries by stop_id which are common in the SSE endpoint';

COMMENT ON INDEX idx_feed_messages_recent IS 
'Partial index for recent feed messages to optimize real-time data queries';