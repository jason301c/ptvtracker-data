-- Migration: Consolidated GTFS-RT Notification System
-- Description: Complete notification system using database triggers only
-- Author: Claude
-- Date: 2025-01-21
-- Note: This replaces all previous notification implementations

-- Drop all old/duplicate functions first
DROP FUNCTION IF EXISTS gtfs_rt.notify_batch_update_efficient CASCADE;
DROP FUNCTION IF EXISTS gtfs_rt.notify_batch_update CASCADE;
DROP FUNCTION IF EXISTS gtfs_rt.get_stop_scheduled_departures CASCADE;
DROP TRIGGER IF EXISTS notify_stop_departure_update ON gtfs_rt.stop_time_updates;

-- Create indexes to support fast real-time queries
CREATE INDEX IF NOT EXISTS idx_stop_time_updates_stop_id_lookup 
ON gtfs_rt.stop_time_updates(stop_id);

CREATE INDEX IF NOT EXISTS idx_trip_updates_timestamp 
ON gtfs_rt.trip_updates(feed_message_id, trip_id);

CREATE INDEX IF NOT EXISTS idx_feed_messages_recent 
ON gtfs_rt.feed_messages(source_id, received_at DESC)
WHERE received_at > NOW() - INTERVAL '2 hours';

-- =========================================
-- STOP TIME UPDATES NOTIFICATIONS
-- =========================================
-- This is the most important trigger for real-time departures
CREATE OR REPLACE FUNCTION gtfs_rt.notify_stop_time_update() RETURNS TRIGGER AS $$
DECLARE
    v_trip_update RECORD;
BEGIN
    -- Get source_id and trip info from the related tables
    SELECT fm.source_id, tu.trip_id, tu.route_id, fm.timestamp
    INTO v_trip_update
    FROM gtfs_rt.trip_updates tu
    JOIN gtfs_rt.feed_messages fm ON tu.feed_message_id = fm.feed_message_id
    WHERE tu.trip_update_id = NEW.trip_update_id;
    
    -- Send notification for this specific stop in the format the frontend expects
    PERFORM pg_notify(
        format('stop_departures:%s:%s', v_trip_update.source_id, NEW.stop_id),
        json_build_object(
            'type', 'update',
            'source_id', v_trip_update.source_id,
            'stop_id', NEW.stop_id,
            'trip_id', v_trip_update.trip_id,
            'route_id', v_trip_update.route_id,
            'arrival_delay', NEW.arrival_delay,
            'departure_delay', NEW.departure_delay,
            'arrival_time', NEW.arrival_time,
            'departure_time', NEW.departure_time,
            'schedule_relationship', NEW.schedule_relationship,
            'timestamp', v_trip_update.timestamp,
            'operation', TG_OP
        )::text
    );
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS stop_time_updates_notify_trigger ON gtfs_rt.stop_time_updates;
CREATE TRIGGER stop_time_updates_notify_trigger
    AFTER INSERT OR UPDATE ON gtfs_rt.stop_time_updates
    FOR EACH ROW
    EXECUTE FUNCTION gtfs_rt.notify_stop_time_update();

-- =========================================
-- VEHICLE POSITIONS NOTIFICATIONS
-- =========================================
CREATE OR REPLACE FUNCTION gtfs_rt.notify_vehicle_position_update() RETURNS TRIGGER AS $$
DECLARE
    v_feed_message RECORD;
BEGIN
    -- Get feed message information
    SELECT fm.source_id, fm.version_id, fm.timestamp
    INTO v_feed_message
    FROM gtfs_rt.feed_messages fm
    WHERE fm.feed_message_id = NEW.feed_message_id;
    
    -- Send vehicle-specific notification
    IF NEW.vehicle_id IS NOT NULL THEN
        PERFORM pg_notify(
            format('vehicle_positions:%s:%s', v_feed_message.source_id, NEW.vehicle_id),
            json_build_object(
                'type', 'vehicle_position',
                'entity_id', NEW.entity_id,
                'trip_id', NEW.trip_id,
                'route_id', NEW.route_id,
                'source_id', v_feed_message.source_id,
                'version_id', v_feed_message.version_id,
                'operation', TG_OP,
                'timestamp', v_feed_message.timestamp,
                'vehicle_id', NEW.vehicle_id,
                'vehicle_label', NEW.vehicle_label,
                'latitude', NEW.latitude,
                'longitude', NEW.longitude,
                'bearing', NEW.bearing,
                'current_status', NEW.current_status,
                'stop_id', NEW.stop_id,
                'schedule_relationship', NEW.schedule_relationship
            )::text
        );
    END IF;
    
    -- Send stop-specific notification if vehicle is at a stop
    IF NEW.stop_id IS NOT NULL THEN
        PERFORM pg_notify(
            format('vehicle_at_stop:%s:%s', v_feed_message.source_id, NEW.stop_id),
            json_build_object(
                'type', 'vehicle_at_stop',
                'stop_id', NEW.stop_id,
                'vehicle_id', NEW.vehicle_id,
                'trip_id', NEW.trip_id,
                'route_id', NEW.route_id,
                'current_status', NEW.current_status,
                'timestamp', v_feed_message.timestamp
            )::text
        );
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS notify_vehicle_position_update ON gtfs_rt.vehicle_positions;
CREATE TRIGGER notify_vehicle_position_update
    AFTER INSERT OR UPDATE ON gtfs_rt.vehicle_positions
    FOR EACH ROW
    EXECUTE FUNCTION gtfs_rt.notify_vehicle_position_update();

-- =========================================
-- SERVICE ALERTS NOTIFICATIONS
-- =========================================
CREATE OR REPLACE FUNCTION gtfs_rt.notify_service_alert_update() RETURNS TRIGGER AS $$
DECLARE
    v_feed_message RECORD;
    v_informed_entity RECORD;
BEGIN
    -- Get feed message information
    SELECT fm.source_id, fm.version_id, fm.timestamp
    INTO v_feed_message
    FROM gtfs_rt.feed_messages fm
    WHERE fm.feed_message_id = NEW.feed_message_id;
    
    -- Send general service alert notification
    PERFORM pg_notify(
        'service_alerts_update',
        json_build_object(
            'type', 'service_alert',
            'alert_id', NEW.alert_id,
            'entity_id', NEW.entity_id,
            'source_id', v_feed_message.source_id,
            'version_id', v_feed_message.version_id,
            'operation', TG_OP,
            'timestamp', v_feed_message.timestamp,
            'cause', NEW.cause,
            'effect', NEW.effect,
            'severity', NEW.severity
        )::text
    );
    
    -- Send specific notifications for affected entities
    FOR v_informed_entity IN
        SELECT DISTINCT route_id, stop_id, agency_id
        FROM gtfs_rt.alert_informed_entities 
        WHERE alert_id = NEW.alert_id
    LOOP
        -- Route-specific alerts
        IF v_informed_entity.route_id IS NOT NULL THEN
            PERFORM pg_notify(
                format('service_alerts_route:%s:%s', v_feed_message.source_id, v_informed_entity.route_id),
                json_build_object(
                    'type', 'service_alert',
                    'alert_id', NEW.alert_id,
                    'route_id', v_informed_entity.route_id,
                    'cause', NEW.cause,
                    'effect', NEW.effect,
                    'severity', NEW.severity,
                    'timestamp', v_feed_message.timestamp
                )::text
            );
        END IF;
        
        -- Stop-specific alerts
        IF v_informed_entity.stop_id IS NOT NULL THEN
            PERFORM pg_notify(
                format('service_alerts_stop:%s:%s', v_feed_message.source_id, v_informed_entity.stop_id),
                json_build_object(
                    'type', 'service_alert',
                    'alert_id', NEW.alert_id,
                    'stop_id', v_informed_entity.stop_id,
                    'cause', NEW.cause,
                    'effect', NEW.effect,
                    'severity', NEW.severity,
                    'timestamp', v_feed_message.timestamp
                )::text
            );
        END IF;
    END LOOP;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS notify_service_alert_update ON gtfs_rt.alerts;
CREATE TRIGGER notify_service_alert_update
    AFTER INSERT OR UPDATE ON gtfs_rt.alerts
    FOR EACH ROW
    EXECUTE FUNCTION gtfs_rt.notify_service_alert_update();

-- =========================================
-- FEED MESSAGES NOTIFICATIONS (for monitoring)
-- =========================================
CREATE OR REPLACE FUNCTION gtfs_rt.notify_feed_message_update() RETURNS TRIGGER AS $$
BEGIN
    -- Send feed update notification for monitoring data freshness
    PERFORM pg_notify(
        format('feed_messages:%s', NEW.source_id),
        json_build_object(
            'type', 'feed_update',
            'feed_message_id', NEW.feed_message_id,
            'source_id', NEW.source_id,
            'version_id', NEW.version_id,
            'feed_type', NEW.feed_type,
            'operation', TG_OP,
            'timestamp', NEW.timestamp,
            'received_at', NEW.received_at,
            'gtfs_realtime_version', NEW.gtfs_realtime_version,
            'incrementality', NEW.incrementality
        )::text
    );
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS notify_feed_message_update ON gtfs_rt.feed_messages;
CREATE TRIGGER notify_feed_message_update
    AFTER INSERT ON gtfs_rt.feed_messages
    FOR EACH ROW
    EXECUTE FUNCTION gtfs_rt.notify_feed_message_update();

-- =========================================
-- COMMENTS
-- =========================================
COMMENT ON FUNCTION gtfs_rt.notify_stop_time_update IS 
'Primary notification function for real-time departures. Sends notifications in the exact format expected by the frontend use-departures.ts hook';

COMMENT ON FUNCTION gtfs_rt.notify_trip_update IS 
'Sends notifications when trip-level updates occur (delays, cancellations, etc)';

COMMENT ON FUNCTION gtfs_rt.notify_vehicle_position_update IS 
'Sends notifications for vehicle position updates, including when vehicles arrive at stops';

COMMENT ON FUNCTION gtfs_rt.notify_service_alert_update IS 
'Sends notifications for service alerts, with specific channels for affected routes and stops';

COMMENT ON FUNCTION gtfs_rt.notify_feed_message_update IS 
'Sends notifications when new feed messages are received, useful for monitoring data freshness';

COMMENT ON INDEX idx_stop_time_updates_stop_id_lookup IS 
'Speeds up real-time queries by stop_id which are common in the SSE endpoint';

COMMENT ON INDEX idx_feed_messages_recent IS 
'Partial index for recent feed messages to optimize real-time data queries';