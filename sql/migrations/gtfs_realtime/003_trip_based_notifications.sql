-- Trip-based NOTIFY system for efficient real-time subscriptions
-- This allows frontend to subscribe only to specific trip_ids instead of entire stops

-- Function to notify trip-specific updates
CREATE OR REPLACE FUNCTION gtfs_rt.notify_trip_update()
RETURNS TRIGGER AS $$
BEGIN
    -- Only notify for INSERT/UPDATE operations on stop_time_updates
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        -- Get trip information from the related trip_update
        DECLARE
            trip_record RECORD;
        BEGIN
            SELECT tu.trip_id, tu.route_id, fm.source_id
            INTO trip_record
            FROM gtfs_rt.trip_updates tu
            JOIN gtfs_rt.feed_messages fm ON tu.feed_message_id = fm.feed_message_id
            WHERE tu.trip_update_id = NEW.trip_update_id;

            -- Send trip-specific notification with minimal payload
            PERFORM pg_notify(
                'trip_updates:' || trip_record.trip_id,
                json_build_object(
                    'trip_id', trip_record.trip_id,
                    'route_id', trip_record.route_id,
                    'source_id', trip_record.source_id,
                    'stop_id', NEW.stop_id,
                    'departure_delay', NEW.departure_delay,
                    'arrival_delay', NEW.arrival_delay,
                    'schedule_relationship', NEW.schedule_relationship,
                    'operation', TG_OP,
                    'timestamp', extract(epoch from now())
                )::text
            );
        EXCEPTION WHEN OTHERS THEN
            -- Log error but don't fail the trigger
            RAISE WARNING 'Error in notify_trip_update: %', SQLERRM;
        END;
    END IF;

    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- Create trigger for trip-based notifications on stop_time_updates
DROP TRIGGER IF EXISTS trigger_notify_trip_update ON gtfs_rt.stop_time_updates;
CREATE TRIGGER trigger_notify_trip_update
    AFTER INSERT OR UPDATE ON gtfs_rt.stop_time_updates
    FOR EACH ROW
    EXECUTE FUNCTION gtfs_rt.notify_trip_update();

-- Function to send batch trip notifications (for backend processors)
CREATE OR REPLACE FUNCTION gtfs_rt.notify_batch_trip_updates(
    trip_ids TEXT[]
) RETURNS VOID AS $$
DECLARE
    trip_id TEXT;
    trip_record RECORD;
BEGIN
    -- Notify for each trip that was updated
    FOREACH trip_id IN ARRAY trip_ids LOOP
        -- Get latest real-time data for this trip
        SELECT 
            tu.trip_id,
            tu.route_id,
            fm.source_id,
            array_agg(DISTINCT stu.stop_id) as stop_ids,
            count(*) as update_count
        INTO trip_record
        FROM gtfs_rt.trip_updates tu
        JOIN gtfs_rt.feed_messages fm ON tu.feed_message_id = fm.feed_message_id
        LEFT JOIN gtfs_rt.stop_time_updates stu ON tu.trip_update_id = stu.trip_update_id
        WHERE tu.trip_id = trip_id
          AND fm.received_at > NOW() - INTERVAL '1 hour'
        GROUP BY tu.trip_id, tu.route_id, fm.source_id
        LIMIT 1;

        -- Send notification if trip found
        IF trip_record.trip_id IS NOT NULL THEN
            PERFORM pg_notify(
                'trip_updates:' || trip_record.trip_id,
                json_build_object(
                    'trip_id', trip_record.trip_id,
                    'route_id', trip_record.route_id,
                    'source_id', trip_record.source_id,
                    'stop_ids', trip_record.stop_ids,
                    'update_count', trip_record.update_count,
                    'operation', 'BATCH_UPDATE',
                    'timestamp', extract(epoch from now())
                )::text
            );
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Helper function to get current real-time data for specific trips
-- Simplified approach - let PostgreSQL infer the exact types from the actual columns
CREATE OR REPLACE FUNCTION gtfs_rt.get_trip_realtime_data(
    trip_ids TEXT[],
    source_id_param INTEGER DEFAULT NULL
) RETURNS SETOF RECORD AS $$
BEGIN
    RETURN QUERY
    SELECT 
        tu.trip_id,
        tu.route_id,
        stu.stop_id,
        stu.departure_delay,
        stu.arrival_delay,
        stu.schedule_relationship,
        fm.received_at
    FROM gtfs_rt.trip_updates tu
    JOIN gtfs_rt.feed_messages fm ON tu.feed_message_id = fm.feed_message_id
    LEFT JOIN gtfs_rt.stop_time_updates stu ON tu.trip_update_id = stu.trip_update_id
    WHERE tu.trip_id = ANY(trip_ids)
      AND (source_id_param IS NULL OR fm.source_id = source_id_param)
      AND fm.received_at > NOW() - INTERVAL '30 minutes'
    ORDER BY tu.trip_id, stu.stop_sequence NULLS LAST;
END;
$$ LANGUAGE plpgsql;