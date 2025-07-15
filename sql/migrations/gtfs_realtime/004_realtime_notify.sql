-- Migration to add NOTIFY functionality for GTFS-realtime updates

-- Create a function to send notifications for realtime updates
CREATE OR REPLACE FUNCTION gtfs_rt.notify_realtime_update()
RETURNS TRIGGER AS $$
DECLARE
    channel_name TEXT;
    payload JSONB;
    feed_msg_id INTEGER;
    source_name TEXT;
    version_id INTEGER;
BEGIN
    -- Determine channel name based on table
    channel_name := 'gtfs_realtime_' || TG_TABLE_NAME;
    
    -- Get feed message ID based on table
    feed_msg_id := COALESCE(NEW.feed_message_id, OLD.feed_message_id);
    
    -- Get source and version info from feed_messages table
    SELECT ts.source_name, fm.version_id 
    INTO source_name, version_id
    FROM gtfs_rt.feed_messages fm
    JOIN gtfs.transport_sources ts ON fm.source_id = ts.source_id
    WHERE fm.feed_message_id = feed_msg_id;
    
    -- Build payload with update information
    payload := jsonb_build_object(
        'table', TG_TABLE_NAME,
        'operation', TG_OP,
        'timestamp', NOW(),
        'source', source_name,
        'version_id', version_id,
        'feed_message_id', feed_msg_id
    );
    
    -- Add specific data based on table
    CASE TG_TABLE_NAME
        WHEN 'vehicle_positions' THEN
            payload := payload || jsonb_build_object(
                'vehicle_id', COALESCE(NEW.vehicle_id, OLD.vehicle_id),
                'trip_id', COALESCE(NEW.trip_id, OLD.trip_id),
                'route_id', COALESCE(NEW.route_id, OLD.route_id),
                'entity_id', COALESCE(NEW.entity_id, OLD.entity_id)
            );
        WHEN 'trip_updates' THEN
            payload := payload || jsonb_build_object(
                'trip_id', COALESCE(NEW.trip_id, OLD.trip_id),
                'route_id', COALESCE(NEW.route_id, OLD.route_id),
                'entity_id', COALESCE(NEW.entity_id, OLD.entity_id)
            );
        WHEN 'alerts' THEN
            payload := payload || jsonb_build_object(
                'alert_id', COALESCE(NEW.alert_id, OLD.alert_id),
                'severity', COALESCE(NEW.severity, OLD.severity),
                'entity_id', COALESCE(NEW.entity_id, OLD.entity_id)
            );
    END CASE;
    
    -- Send notification
    PERFORM pg_notify(channel_name, payload::TEXT);
    
    -- Also send a general update notification
    PERFORM pg_notify('gtfs_realtime_update', payload::TEXT);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for each realtime table
DROP TRIGGER IF EXISTS notify_vehicle_positions_update ON gtfs_rt.vehicle_positions;
CREATE TRIGGER notify_vehicle_positions_update
    AFTER INSERT OR UPDATE
    ON gtfs_rt.vehicle_positions
    FOR EACH ROW
    EXECUTE FUNCTION gtfs_rt.notify_realtime_update();

DROP TRIGGER IF EXISTS notify_trip_updates_update ON gtfs_rt.trip_updates;
CREATE TRIGGER notify_trip_updates_update
    AFTER INSERT OR UPDATE
    ON gtfs_rt.trip_updates
    FOR EACH ROW
    EXECUTE FUNCTION gtfs_rt.notify_realtime_update();

DROP TRIGGER IF EXISTS notify_alerts_update ON gtfs_rt.alerts;
CREATE TRIGGER notify_alerts_update
    AFTER INSERT OR UPDATE
    ON gtfs_rt.alerts
    FOR EACH ROW
    EXECUTE FUNCTION gtfs_rt.notify_realtime_update();

-- Function to send batch update notification (called after bulk inserts)
CREATE OR REPLACE FUNCTION gtfs_rt.notify_batch_update(
    p_table_name TEXT,
    p_source TEXT,
    p_version_id BIGINT,
    p_record_count INTEGER
)
RETURNS VOID AS $$
DECLARE
    payload JSONB;
BEGIN
    payload := jsonb_build_object(
        'table', p_table_name,
        'operation', 'BATCH_INSERT',
        'timestamp', NOW(),
        'source', p_source,
        'version_id', p_version_id,
        'record_count', p_record_count
    );
    
    -- Send specific and general notifications
    PERFORM pg_notify('gtfs_realtime_' || p_table_name || '_batch', payload::TEXT);
    PERFORM pg_notify('gtfs_realtime_batch_update', payload::TEXT);
END;
$$ LANGUAGE plpgsql;

-- Comment on the functions
COMMENT ON FUNCTION gtfs_rt.notify_realtime_update() IS 'Sends PostgreSQL NOTIFY messages when GTFS realtime data is updated';
COMMENT ON FUNCTION gtfs_rt.notify_batch_update(TEXT, TEXT, BIGINT, INTEGER) IS 'Sends PostgreSQL NOTIFY messages after batch updates of GTFS realtime data';