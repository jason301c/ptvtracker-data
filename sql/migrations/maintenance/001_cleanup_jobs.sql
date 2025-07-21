-- PTV Tracker Database Maintenance and Cleanup Jobs
-- This file contains functions and procedures for routine database maintenance

-- Function to clean up old real-time data
-- Real-time data older than 7 days is generally not useful for transport tracking
CREATE OR REPLACE FUNCTION gtfs_rt.cleanup_old_realtime_data(retention_days INTEGER DEFAULT 7)
RETURNS TABLE(
  table_name TEXT,
  records_deleted BIGINT,
  size_freed TEXT
) 
LANGUAGE plpgsql AS $$
DECLARE
  cutoff_timestamp TIMESTAMPTZ;
  deleted_count BIGINT;
  size_before BIGINT;
  size_after BIGINT;
BEGIN
  -- Calculate cutoff timestamp
  cutoff_timestamp := NOW() - (retention_days || ' days')::INTERVAL;
  
  RAISE NOTICE 'Starting cleanup of real-time data older than %', cutoff_timestamp;
  
  -- Clean up vehicle_positions (via feed_messages timestamp)
  size_before := pg_total_relation_size('gtfs_rt.vehicle_positions');
  
  DELETE FROM gtfs_rt.vehicle_positions 
  WHERE feed_message_id IN (
    SELECT feed_message_id 
    FROM gtfs_rt.feed_messages 
    WHERE timestamp < cutoff_timestamp
  );
  
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  size_after := pg_total_relation_size('gtfs_rt.vehicle_positions');
  
  table_name := 'vehicle_positions';
  records_deleted := deleted_count;
  size_freed := pg_size_pretty(size_before - size_after);
  RETURN NEXT;
  
  -- Clean up stop_time_updates (via trip_updates timestamp)
  size_before := pg_total_relation_size('gtfs_rt.stop_time_updates');
  
  DELETE FROM gtfs_rt.stop_time_updates 
  WHERE trip_update_id IN (
    SELECT trip_update_id 
    FROM gtfs_rt.trip_updates 
    WHERE timestamp < cutoff_timestamp
  );
  
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  size_after := pg_total_relation_size('gtfs_rt.stop_time_updates');
  
  table_name := 'stop_time_updates';
  records_deleted := deleted_count;
  size_freed := pg_size_pretty(size_before - size_after);
  RETURN NEXT;
  
  -- Clean up trip_updates
  size_before := pg_total_relation_size('gtfs_rt.trip_updates');
  
  DELETE FROM gtfs_rt.trip_updates 
  WHERE timestamp < cutoff_timestamp;
  
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  size_after := pg_total_relation_size('gtfs_rt.trip_updates');
  
  table_name := 'trip_updates';
  records_deleted := deleted_count;
  size_freed := pg_size_pretty(size_before - size_after);
  RETURN NEXT;
  
  -- Clean up alerts with old active periods
  size_before := pg_total_relation_size('gtfs_rt.alerts');
  
  DELETE FROM gtfs_rt.alerts 
  WHERE alert_id IN (
    SELECT DISTINCT a.alert_id 
    FROM gtfs_rt.alerts a
    JOIN gtfs_rt.alert_active_periods aap ON a.alert_id = aap.alert_id
    WHERE COALESCE(aap.end_time, EXTRACT(EPOCH FROM NOW())::BIGINT) < EXTRACT(EPOCH FROM cutoff_timestamp)::BIGINT
    AND NOT EXISTS (
      -- Keep alerts that have at least one active period that's still current
      SELECT 1 FROM gtfs_rt.alert_active_periods aap2 
      WHERE aap2.alert_id = a.alert_id 
      AND COALESCE(aap2.end_time, EXTRACT(EPOCH FROM NOW())::BIGINT) >= EXTRACT(EPOCH FROM cutoff_timestamp)::BIGINT
    )
  );
  
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  size_after := pg_total_relation_size('gtfs_rt.alerts');
  
  table_name := 'alerts';
  records_deleted := deleted_count;
  size_freed := pg_size_pretty(size_before - size_after);
  RETURN NEXT;
  
  -- Clean up orphaned feed_messages (should be last)
  size_before := pg_total_relation_size('gtfs_rt.feed_messages');
  
  DELETE FROM gtfs_rt.feed_messages 
  WHERE timestamp < cutoff_timestamp;
  
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  size_after := pg_total_relation_size('gtfs_rt.feed_messages');
  
  table_name := 'feed_messages';
  records_deleted := deleted_count;
  size_freed := pg_size_pretty(size_before - size_after);
  RETURN NEXT;
  
  -- Vacuum analyze the cleaned tables
  VACUUM ANALYZE gtfs_rt.vehicle_positions;
  VACUUM ANALYZE gtfs_rt.stop_time_updates;
  VACUUM ANALYZE gtfs_rt.trip_updates;
  VACUUM ANALYZE gtfs_rt.alerts;
  VACUUM ANALYZE gtfs_rt.feed_messages;
  
  RAISE NOTICE 'Cleanup completed successfully. Tables vacuumed and analyzed.';
END;
$$;

-- Function to get database health summary
CREATE OR REPLACE FUNCTION public.get_database_health_summary()
RETURNS TABLE(
  metric TEXT,
  value TEXT,
  status TEXT,
  recommendation TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
  -- Database size metrics
  RETURN QUERY
  SELECT 
    'Total Database Size'::TEXT,
    pg_size_pretty(pg_database_size(current_database()))::TEXT,
    CASE 
      WHEN pg_database_size(current_database()) > 1000000000000 THEN 'WARNING'  -- 1TB
      WHEN pg_database_size(current_database()) > 500000000000 THEN 'CAUTION'   -- 500GB
      ELSE 'OK'
    END::TEXT,
    CASE 
      WHEN pg_database_size(current_database()) > 1000000000000 THEN 'Consider archiving old data'
      WHEN pg_database_size(current_database()) > 500000000000 THEN 'Monitor growth, plan cleanup'
      ELSE 'Size is acceptable'
    END::TEXT;
  
  -- Real-time data age
  RETURN QUERY
  SELECT 
    'Oldest Real-time Data'::TEXT,
    (
      SELECT EXTRACT(days FROM NOW() - MIN(timestamp))::TEXT || ' days ago'
      FROM gtfs_rt.trip_updates 
      WHERE timestamp IS NOT NULL
    )::TEXT,
    CASE 
      WHEN (
        SELECT EXTRACT(days FROM NOW() - MIN(timestamp))
        FROM gtfs_rt.trip_updates 
        WHERE timestamp IS NOT NULL
      ) > 30 THEN 'WARNING'
      WHEN (
        SELECT EXTRACT(days FROM NOW() - MIN(timestamp))
        FROM gtfs_rt.trip_updates 
        WHERE timestamp IS NOT NULL
      ) > 7 THEN 'CAUTION'
      ELSE 'OK'
    END::TEXT,
    CASE 
      WHEN (
        SELECT EXTRACT(days FROM NOW() - MIN(timestamp))
        FROM gtfs_rt.trip_updates 
        WHERE timestamp IS NOT NULL
      ) > 30 THEN 'Run cleanup: SELECT * FROM gtfs_rt.cleanup_old_realtime_data(7);'
      WHEN (
        SELECT EXTRACT(days FROM NOW() - MIN(timestamp))
        FROM gtfs_rt.trip_updates 
        WHERE timestamp IS NOT NULL
      ) > 7 THEN 'Consider running cleanup soon'
      ELSE 'Data freshness is good'
    END::TEXT;
    
  -- Large table sizes
  RETURN QUERY
  SELECT 
    'Largest Table'::TEXT,
    (
      SELECT schemaname || '.' || relname || ' (' || pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) || ')'
      FROM pg_tables 
      WHERE schemaname IN ('gtfs', 'gtfs_rt', 'public')
      ORDER BY pg_total_relation_size(schemaname||'.'||relname) DESC 
      LIMIT 1
    )::TEXT,
    'INFO'::TEXT,
    'Monitor for unexpected growth'::TEXT;
END;
$$;

-- Utility function to refresh materialized views
CREATE OR REPLACE FUNCTION public.refresh_materialized_views()
RETURNS TABLE(
  view_name TEXT,
  refresh_status TEXT,
  duration INTERVAL
)
LANGUAGE plpgsql AS $$
DECLARE
  mv_record RECORD;
  start_time TIMESTAMPTZ;
  end_time TIMESTAMPTZ;
BEGIN
  -- Refresh all materialized views in the gtfs schema
  FOR mv_record IN 
    SELECT schemaname, matviewname 
    FROM pg_matviews 
    WHERE schemaname = 'gtfs'
    ORDER BY matviewname
  LOOP
    start_time := clock_timestamp();
    
    BEGIN
      EXECUTE format('REFRESH MATERIALIZED VIEW %I.%I', mv_record.schemaname, mv_record.matviewname);
      end_time := clock_timestamp();
      
      view_name := mv_record.schemaname || '.' || mv_record.matviewname;
      refresh_status := 'SUCCESS';
      duration := end_time - start_time;
      RETURN NEXT;
      
    EXCEPTION WHEN OTHERS THEN
      end_time := clock_timestamp();
      
      view_name := mv_record.schemaname || '.' || mv_record.matviewname;
      refresh_status := 'ERROR: ' || SQLERRM;
      duration := end_time - start_time;
      RETURN NEXT;
    END;
  END LOOP;
END;
$$;

-- Add comments for documentation
COMMENT ON FUNCTION gtfs_rt.cleanup_old_realtime_data(INTEGER) IS 
'Cleans up real-time data older than specified days (default 7). Run weekly via cron job.';

COMMENT ON FUNCTION public.get_database_health_summary() IS 
'Returns database health metrics and recommendations. Run daily for monitoring.';

COMMENT ON FUNCTION public.refresh_materialized_views() IS 
'Refreshes all materialized views in the gtfs schema. Run when GTFS data is updated.';

-- Function to clean up old inactive GTFS versions
-- Keeps the active version and optionally a specified number of recent inactive versions
CREATE OR REPLACE FUNCTION gtfs.cleanup_old_versions(keep_inactive_versions INTEGER DEFAULT 1)
RETURNS TABLE(
  version_id INTEGER,
  version_name TEXT,
  records_deleted BIGINT,
  size_freed TEXT,
  cleanup_status TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
  version_record RECORD;
  deleted_count BIGINT;
  size_before BIGINT;
  size_after BIGINT;
  total_size_before BIGINT;
  total_size_after BIGINT;
  gtfs_table RECORD;
BEGIN
  -- Get total GTFS schema size before cleanup
  SELECT pg_total_relation_size('gtfs.stop_times') + 
         pg_total_relation_size('gtfs.shapes') +
         pg_total_relation_size('gtfs.trips') +
         pg_total_relation_size('gtfs.stops') +
         pg_total_relation_size('gtfs.routes') +
         pg_total_relation_size('gtfs.calendar') +
         pg_total_relation_size('gtfs.calendar_dates') +
         pg_total_relation_size('gtfs.agency') +
         pg_total_relation_size('gtfs.transfers') +
         pg_total_relation_size('gtfs.pathways') +
         pg_total_relation_size('gtfs.levels')
  INTO total_size_before;

  -- Get versions to delete (inactive versions beyond the keep limit)
  FOR version_record IN 
    SELECT v.version_id, v.version_name, v.created_at
    FROM gtfs.versions v
    WHERE v.is_active = false
    ORDER BY v.created_at DESC
    OFFSET keep_inactive_versions
  LOOP
    RAISE NOTICE 'Cleaning up version %: % (created: %)', 
                 version_record.version_id, 
                 version_record.version_name,
                 version_record.created_at;
    
    deleted_count := 0;
    
    -- Delete from all GTFS tables for this version
    -- Order matters due to foreign key constraints
    
    -- Delete stop_times first (largest table, references trips)
    DELETE FROM gtfs.stop_times WHERE version_id = version_record.version_id;
    GET DIAGNOSTICS size_before = ROW_COUNT;
    deleted_count := deleted_count + size_before;
    
    -- Delete trips (references routes, calendar, shapes)
    DELETE FROM gtfs.trips WHERE version_id = version_record.version_id;
    GET DIAGNOSTICS size_before = ROW_COUNT;
    deleted_count := deleted_count + size_before;
    
    -- Delete shapes
    DELETE FROM gtfs.shapes WHERE version_id = version_record.version_id;
    GET DIAGNOSTICS size_before = ROW_COUNT;
    deleted_count := deleted_count + size_before;
    
    -- Delete calendar_dates
    DELETE FROM gtfs.calendar_dates WHERE version_id = version_record.version_id;
    GET DIAGNOSTICS size_before = ROW_COUNT;
    deleted_count := deleted_count + size_before;
    
    -- Delete calendar
    DELETE FROM gtfs.calendar WHERE version_id = version_record.version_id;
    GET DIAGNOSTICS size_before = ROW_COUNT;
    deleted_count := deleted_count + size_before;
    
    -- Delete transfers
    DELETE FROM gtfs.transfers WHERE version_id = version_record.version_id;
    GET DIAGNOSTICS size_before = ROW_COUNT;
    deleted_count := deleted_count + size_before;
    
    -- Delete pathways
    DELETE FROM gtfs.pathways WHERE version_id = version_record.version_id;
    GET DIAGNOSTICS size_before = ROW_COUNT;
    deleted_count := deleted_count + size_before;
    
    -- Delete levels
    DELETE FROM gtfs.levels WHERE version_id = version_record.version_id;
    GET DIAGNOSTICS size_before = ROW_COUNT;
    deleted_count := deleted_count + size_before;
    
    -- Delete stops
    DELETE FROM gtfs.stops WHERE version_id = version_record.version_id;
    GET DIAGNOSTICS size_before = ROW_COUNT;
    deleted_count := deleted_count + size_before;
    
    -- Delete routes
    DELETE FROM gtfs.routes WHERE version_id = version_record.version_id;
    GET DIAGNOSTICS size_before = ROW_COUNT;
    deleted_count := deleted_count + size_before;
    
    -- Delete agency
    DELETE FROM gtfs.agency WHERE version_id = version_record.version_id;
    GET DIAGNOSTICS size_before = ROW_COUNT;
    deleted_count := deleted_count + size_before;
    
    -- Finally delete the version record itself
    DELETE FROM gtfs.versions WHERE version_id = version_record.version_id;
    
    -- Return cleanup result for this version
    version_id := version_record.version_id;
    version_name := version_record.version_name;
    records_deleted := deleted_count;
    cleanup_status := 'SUCCESS';
    size_freed := 'Calculated after all versions cleaned';
    RETURN NEXT;
    
  END LOOP;
  
  -- Get total GTFS schema size after cleanup
  SELECT pg_total_relation_size('gtfs.stop_times') + 
         pg_total_relation_size('gtfs.shapes') +
         pg_total_relation_size('gtfs.trips') +
         pg_total_relation_size('gtfs.stops') +
         pg_total_relation_size('gtfs.routes') +
         pg_total_relation_size('gtfs.calendar') +
         pg_total_relation_size('gtfs.calendar_dates') +
         pg_total_relation_size('gtfs.agency') +
         pg_total_relation_size('gtfs.transfers') +
         pg_total_relation_size('gtfs.pathways') +
         pg_total_relation_size('gtfs.levels')
  INTO total_size_after;
  
  -- Vacuum analyze all GTFS tables
  VACUUM ANALYZE gtfs.stop_times;
  VACUUM ANALYZE gtfs.shapes;
  VACUUM ANALYZE gtfs.trips;
  VACUUM ANALYZE gtfs.stops;
  VACUUM ANALYZE gtfs.routes;
  VACUUM ANALYZE gtfs.calendar;
  VACUUM ANALYZE gtfs.calendar_dates;
  VACUUM ANALYZE gtfs.agency;
  VACUUM ANALYZE gtfs.transfers;
  VACUUM ANALYZE gtfs.pathways;
  VACUUM ANALYZE gtfs.levels;
  VACUUM ANALYZE gtfs.versions;
  
  -- Return summary record with total space freed
  version_id := NULL;
  version_name := 'CLEANUP_SUMMARY';
  records_deleted := NULL;
  cleanup_status := 'COMPLETED';
  size_freed := pg_size_pretty(total_size_before - total_size_after);
  RETURN NEXT;
  
  RAISE NOTICE 'GTFS version cleanup completed. Total space freed: %', 
               pg_size_pretty(total_size_before - total_size_after);
END;
$$;

-- Function to list GTFS versions with size information
CREATE OR REPLACE FUNCTION gtfs.list_versions_with_sizes()
RETURNS TABLE(
  version_id INTEGER,
  version_name TEXT,
  created_at TIMESTAMPTZ,
  is_active BOOLEAN,
  stop_times_count BIGINT,
  trips_count BIGINT,
  stops_count BIGINT,
  estimated_size TEXT,
  age_days INTEGER
)
LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  SELECT 
    v.version_id,
    v.version_name,
    v.created_at,
    v.is_active,
    COALESCE(st.stop_times_count, 0) as stop_times_count,
    COALESCE(t.trips_count, 0) as trips_count,
    COALESCE(s.stops_count, 0) as stops_count,
    CASE 
      WHEN COALESCE(st.stop_times_count, 0) > 0 THEN
        pg_size_pretty(
          -- Estimate based on stop_times table size (usually largest)
          (pg_total_relation_size('gtfs.stop_times') * COALESCE(st.stop_times_count, 0) / 
           NULLIF((SELECT COUNT(*) FROM gtfs.stop_times), 0)) +
          -- Add estimated size for other tables (rough approximation)
          (pg_total_relation_size('gtfs.trips') * COALESCE(t.trips_count, 0) / 
           NULLIF((SELECT COUNT(*) FROM gtfs.trips), 0)) +
          (pg_total_relation_size('gtfs.stops') * COALESCE(s.stops_count, 0) / 
           NULLIF((SELECT COUNT(*) FROM gtfs.stops), 0))
        )
      ELSE '0 bytes'
    END as estimated_size,
    EXTRACT(days FROM NOW() - v.created_at)::INTEGER as age_days
  FROM gtfs.versions v
  LEFT JOIN (
    SELECT version_id, COUNT(*) as stop_times_count 
    FROM gtfs.stop_times 
    GROUP BY version_id
  ) st ON v.version_id = st.version_id
  LEFT JOIN (
    SELECT version_id, COUNT(*) as trips_count 
    FROM gtfs.trips 
    GROUP BY version_id
  ) t ON v.version_id = t.version_id
  LEFT JOIN (
    SELECT version_id, COUNT(*) as stops_count 
    FROM gtfs.stops 
    GROUP BY version_id
  ) s ON v.version_id = s.version_id
  ORDER BY v.created_at DESC;
END;
$$;

-- Add comments for the new functions
COMMENT ON FUNCTION gtfs.cleanup_old_versions(INTEGER) IS 
'Removes old inactive GTFS versions, keeping only the active version and a specified number of recent inactive versions (default: 1). Run after importing new GTFS data.';

COMMENT ON FUNCTION gtfs.list_versions_with_sizes() IS 
'Lists all GTFS versions with record counts and estimated storage sizes. Use to identify versions that can be cleaned up.';