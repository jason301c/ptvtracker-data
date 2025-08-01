-- PTV Tracker Database Maintenance and Cleanup Jobs
-- This file contains functions and procedures for routine database maintenance

-- Function to clean up old real-time data in batches
-- Real-time data older than 7 days is generally not useful for transport tracking
-- This batched approach prevents long-running transactions that can cause connection timeouts
CREATE OR REPLACE FUNCTION gtfs_rt.cleanup_old_realtime_data_batch(
  retention_days INTEGER DEFAULT 7,
  batch_size INTEGER DEFAULT 10000
)
RETURNS TABLE(
  table_name TEXT,
  batch_number INTEGER,
  records_deleted BIGINT,
  size_freed TEXT,
  batch_duration INTERVAL
) 
LANGUAGE plpgsql AS $$
DECLARE
  cutoff_timestamp TIMESTAMPTZ;
  deleted_count BIGINT;
  size_before BIGINT;
  size_after BIGINT;
  batch_num INTEGER;
  start_time TIMESTAMPTZ;
  end_time TIMESTAMPTZ;
  total_deleted BIGINT;
  min_timestamp TIMESTAMPTZ;
  batch_end_timestamp TIMESTAMPTZ;
BEGIN
  -- Calculate cutoff timestamp
  cutoff_timestamp := NOW() - (retention_days || ' days')::INTERVAL;
  
  RAISE NOTICE 'Starting batched cleanup of real-time data older than % with batch size %', cutoff_timestamp, batch_size;
  
  -- Clean up trip_updates first (most efficient, direct timestamp)
  size_before := pg_total_relation_size('gtfs_rt.trip_updates');
  batch_num := 0;
  total_deleted := 0;
  
  -- Get the minimum timestamp to process in time-based batches
  SELECT MIN(timestamp) INTO min_timestamp 
  FROM gtfs_rt.trip_updates 
  WHERE timestamp < cutoff_timestamp;
  
  IF min_timestamp IS NOT NULL THEN
    batch_end_timestamp := min_timestamp;
    
    LOOP
      start_time := clock_timestamp();
      batch_num := batch_num + 1;
      
      -- Process time-based batches (more efficient than LIMIT with large offsets)
      batch_end_timestamp := batch_end_timestamp + (batch_size::TEXT || ' seconds')::INTERVAL;
      IF batch_end_timestamp > cutoff_timestamp THEN
        batch_end_timestamp := cutoff_timestamp;
      END IF;
      
      DELETE FROM gtfs_rt.trip_updates 
      WHERE timestamp >= (batch_end_timestamp - (batch_size::TEXT || ' seconds')::INTERVAL) 
        AND timestamp < batch_end_timestamp;
      
      GET DIAGNOSTICS deleted_count = ROW_COUNT;
      total_deleted := total_deleted + deleted_count;
      end_time := clock_timestamp();
      
      IF deleted_count > 0 THEN
        table_name := 'trip_updates';
        batch_number := batch_num;
        records_deleted := deleted_count;
        size_freed := 'In progress';
        batch_duration := end_time - start_time;
        RETURN NEXT;
        
        PERFORM pg_sleep(0.05);
      END IF;
      
      -- Exit if we've processed all data up to cutoff
      EXIT WHEN batch_end_timestamp >= cutoff_timestamp;
    END LOOP;
  END IF;
  
  size_after := pg_total_relation_size('gtfs_rt.trip_updates');
  table_name := 'trip_updates';
  batch_number := 0;
  records_deleted := total_deleted;
  size_freed := pg_size_pretty(size_before - size_after);
  batch_duration := NULL;
  RETURN NEXT;
  
  -- Clean up stop_time_updates (now that trip_updates are gone)
  size_before := pg_total_relation_size('gtfs_rt.stop_time_updates');
  batch_num := 0;
  total_deleted := 0;
  
  LOOP
    start_time := clock_timestamp();
    batch_num := batch_num + 1;
    
    -- Delete orphaned stop_time_updates (trip_updates already deleted)
    DELETE FROM gtfs_rt.stop_time_updates 
    WHERE trip_update_id NOT IN (
      SELECT trip_update_id FROM gtfs_rt.trip_updates
    )
    AND trip_update_id IN (
      SELECT trip_update_id FROM gtfs_rt.stop_time_updates 
      LIMIT batch_size
    );
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    total_deleted := total_deleted + deleted_count;
    end_time := clock_timestamp();
    
    IF deleted_count > 0 THEN
      table_name := 'stop_time_updates';
      batch_number := batch_num;
      records_deleted := deleted_count;
      size_freed := 'In progress';
      batch_duration := end_time - start_time;
      RETURN NEXT;
      
      PERFORM pg_sleep(0.05);
    ELSE
      EXIT;
    END IF;
  END LOOP;
  
  size_after := pg_total_relation_size('gtfs_rt.stop_time_updates');
  table_name := 'stop_time_updates';
  batch_number := 0;
  records_deleted := total_deleted;
  size_freed := pg_size_pretty(size_before - size_after);
  batch_duration := NULL;
  RETURN NEXT;
  
  -- Clean up vehicle_positions using feed_messages timestamp approach
  size_before := pg_total_relation_size('gtfs_rt.vehicle_positions');
  batch_num := 0;
  total_deleted := 0;
  
  LOOP
    start_time := clock_timestamp();
    batch_num := batch_num + 1;
    
    -- Create a temporary table with old feed_message_ids for efficient deletion
    CREATE TEMP TABLE IF NOT EXISTS old_feed_messages AS 
    SELECT feed_message_id 
    FROM gtfs_rt.feed_messages 
    WHERE timestamp < cutoff_timestamp 
    LIMIT batch_size * 10; -- Get more feed message IDs per batch
    
    DELETE FROM gtfs_rt.vehicle_positions 
    WHERE feed_message_id IN (
      SELECT feed_message_id FROM old_feed_messages LIMIT batch_size
    );
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    total_deleted := total_deleted + deleted_count;
    end_time := clock_timestamp();
    
    -- Clean up temp table for next iteration
    DELETE FROM old_feed_messages 
    WHERE feed_message_id IN (
      SELECT feed_message_id FROM old_feed_messages LIMIT batch_size
    );
    
    IF deleted_count > 0 THEN
      table_name := 'vehicle_positions';
      batch_number := batch_num;
      records_deleted := deleted_count;
      size_freed := 'In progress';
      batch_duration := end_time - start_time;
      RETURN NEXT;
      
      PERFORM pg_sleep(0.05);
    ELSE
      -- Drop temp table and exit
      DROP TABLE IF EXISTS old_feed_messages;
      EXIT;
    END IF;
  END LOOP;
  
  size_after := pg_total_relation_size('gtfs_rt.vehicle_positions');
  table_name := 'vehicle_positions';
  batch_number := 0;
  records_deleted := total_deleted;
  size_freed := pg_size_pretty(size_before - size_after);
  batch_duration := NULL;
  RETURN NEXT;
  
  -- Clean up alerts (simplified - delete all old alerts)
  size_before := pg_total_relation_size('gtfs_rt.alerts');
  start_time := clock_timestamp();
  
  DELETE FROM gtfs_rt.alerts 
  WHERE alert_id IN (
    SELECT DISTINCT a.alert_id 
    FROM gtfs_rt.alerts a
    JOIN gtfs_rt.alert_active_periods aap ON a.alert_id = aap.alert_id
    WHERE COALESCE(aap.end_time, EXTRACT(EPOCH FROM NOW())::BIGINT) < EXTRACT(EPOCH FROM cutoff_timestamp)::BIGINT
    AND NOT EXISTS (
      SELECT 1 FROM gtfs_rt.alert_active_periods aap2 
      WHERE aap2.alert_id = a.alert_id 
      AND COALESCE(aap2.end_time, EXTRACT(EPOCH FROM NOW())::BIGINT) >= EXTRACT(EPOCH FROM cutoff_timestamp)::BIGINT
    )
  );
  
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  end_time := clock_timestamp();
  
  size_after := pg_total_relation_size('gtfs_rt.alerts');
  table_name := 'alerts';
  batch_number := 1;
  records_deleted := deleted_count;
  size_freed := pg_size_pretty(size_before - size_after);
  batch_duration := end_time - start_time;
  RETURN NEXT;
  
  -- Clean up feed_messages last (timestamp-based, efficient)
  size_before := pg_total_relation_size('gtfs_rt.feed_messages');
  start_time := clock_timestamp();
  
  DELETE FROM gtfs_rt.feed_messages 
  WHERE timestamp < cutoff_timestamp;
  
  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  end_time := clock_timestamp();
  
  size_after := pg_total_relation_size('gtfs_rt.feed_messages');
  table_name := 'feed_messages';
  batch_number := 1;
  records_deleted := deleted_count;
  size_freed := pg_size_pretty(size_before - size_after);
  batch_duration := end_time - start_time;
  RETURN NEXT;
  
  RAISE NOTICE 'Batched cleanup completed successfully. Run gtfs_rt.vacuum_cleanup_tables() separately to vacuum and analyze tables.';
END;
$$;

-- Function to vacuum and analyze tables after cleanup (must be run outside transaction)
CREATE OR REPLACE FUNCTION gtfs_rt.vacuum_cleanup_tables()
RETURNS TABLE(
  table_name TEXT,
  operation TEXT,
  duration INTERVAL,
  status TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
  start_time TIMESTAMPTZ;
  end_time TIMESTAMPTZ;
  table_list TEXT[] := ARRAY['vehicle_positions', 'stop_time_updates', 'trip_updates', 'alerts', 'feed_messages'];
  tbl TEXT;
BEGIN
  RAISE NOTICE 'Starting VACUUM ANALYZE of cleanup tables';
  
  FOREACH tbl IN ARRAY table_list
  LOOP
    start_time := clock_timestamp();
    
    BEGIN
      EXECUTE format('VACUUM ANALYZE gtfs_rt.%I', tbl);
      end_time := clock_timestamp();
      
      table_name := tbl;
      operation := 'VACUUM ANALYZE';
      duration := end_time - start_time;
      status := 'SUCCESS';
      RETURN NEXT;
      
    EXCEPTION WHEN OTHERS THEN
      end_time := clock_timestamp();
      
      table_name := tbl;
      operation := 'VACUUM ANALYZE';
      duration := end_time - start_time;
      status := 'ERROR: ' || SQLERRM;
      RETURN NEXT;
    END;
  END LOOP;
  
  RAISE NOTICE 'VACUUM ANALYZE completed for all tables';
END;
$$;

-- Keep the original function for backwards compatibility but mark as deprecated
CREATE OR REPLACE FUNCTION gtfs_rt.cleanup_old_realtime_data(retention_days INTEGER DEFAULT 7)
RETURNS TABLE(
  table_name TEXT,
  records_deleted BIGINT,
  size_freed TEXT
) 
LANGUAGE plpgsql AS $$
BEGIN
  RAISE WARNING 'cleanup_old_realtime_data is deprecated. Use cleanup_old_realtime_data_batch for better performance.';
  
  -- Call the batched version and aggregate results
  RETURN QUERY
  SELECT 
    c.table_name,
    SUM(c.records_deleted) as records_deleted,
    MAX(c.size_freed) as size_freed
  FROM gtfs_rt.cleanup_old_realtime_data_batch(retention_days, 5000) c
  WHERE c.batch_number = 0  -- Only summary records
  GROUP BY c.table_name;
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
COMMENT ON FUNCTION gtfs_rt.cleanup_old_realtime_data_batch(INTEGER, INTEGER) IS 
'Cleans up real-time data older than specified days (default 7) using batched processing to prevent connection timeouts. batch_size controls records per batch (default 10000). Run weekly via cron job.';

COMMENT ON FUNCTION gtfs_rt.cleanup_old_realtime_data(INTEGER) IS 
'DEPRECATED: Use cleanup_old_realtime_data_batch instead. Cleans up real-time data older than specified days (default 7).';

COMMENT ON FUNCTION gtfs_rt.vacuum_cleanup_tables() IS 
'Runs VACUUM ANALYZE on all real-time cleanup tables. Must be called outside of transactions. Run after cleanup_old_realtime_data_batch for optimal performance.';

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
    SELECT 
      version_record.version_id,
      version_record.version_name,
      deleted_count,
      'Calculated after all versions cleaned',
      'SUCCESS'
    INTO version_id, version_name, records_deleted, size_freed, cleanup_status;
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
  
  -- Note: VACUUM ANALYZE should be run separately after cleanup to avoid transaction block issues
  
  -- Return summary record with total space freed
  SELECT 
    NULL::INTEGER,
    'CLEANUP_SUMMARY',
    NULL::BIGINT,
    pg_size_pretty(total_size_before - total_size_after),
    'COMPLETED'
  INTO version_id, version_name, records_deleted, size_freed, cleanup_status;
  RETURN NEXT;
  
  RAISE NOTICE 'GTFS version cleanup completed. Total space freed: %', 
               pg_size_pretty(total_size_before - total_size_after);
END;
$$;

-- Function to vacuum and analyze GTFS tables after version cleanup (must be run outside transaction)
CREATE OR REPLACE FUNCTION gtfs.vacuum_gtfs_tables()
RETURNS TABLE(
  table_name TEXT,
  operation TEXT,
  duration INTERVAL,
  status TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
  start_time TIMESTAMPTZ;
  end_time TIMESTAMPTZ;
  table_list TEXT[] := ARRAY['stop_times', 'shapes', 'trips', 'stops', 'routes', 'calendar', 'calendar_dates', 'agency', 'transfers', 'pathways', 'levels', 'versions'];
  tbl TEXT;
BEGIN
  RAISE NOTICE 'Starting VACUUM ANALYZE of GTFS tables';
  
  FOREACH tbl IN ARRAY table_list
  LOOP
    start_time := clock_timestamp();
    
    BEGIN
      EXECUTE format('VACUUM ANALYZE gtfs.%I', tbl);
      end_time := clock_timestamp();
      
      table_name := tbl;
      operation := 'VACUUM ANALYZE';
      duration := end_time - start_time;
      status := 'SUCCESS';
      RETURN NEXT;
      
    EXCEPTION WHEN OTHERS THEN
      end_time := clock_timestamp();
      
      table_name := tbl;
      operation := 'VACUUM ANALYZE';
      duration := end_time - start_time;
      status := 'ERROR: ' || SQLERRM;
      RETURN NEXT;
    END;
  END LOOP;
  
  RAISE NOTICE 'VACUUM ANALYZE completed for all GTFS tables';
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

COMMENT ON FUNCTION gtfs.vacuum_gtfs_tables() IS 
'Runs VACUUM ANALYZE on all GTFS tables. Must be called outside of transactions. Run after cleanup_old_versions for optimal performance.';

COMMENT ON FUNCTION gtfs.list_versions_with_sizes() IS 
'Lists all GTFS versions with record counts and estimated storage sizes. Use to identify versions that can be cleaned up.';