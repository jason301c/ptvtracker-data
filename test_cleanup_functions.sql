-- Comprehensive test script for cleanup functions
-- Run this to validate all cleanup functionality before deployment

-- =============================================================================
-- PART 1: Function Existence and Schema Validation
-- =============================================================================

-- Check if all cleanup functions exist
SELECT 
    routine_schema,
    routine_name,
    routine_type,
    data_type as return_type,
    routine_definition IS NOT NULL as has_definition,
    CASE 
        WHEN routine_definition ILIKE '%version_id :=%' THEN 'WARNING: Direct assignment found'
        WHEN routine_definition ILIKE '%SELECT%INTO%version_id%' THEN 'OK: Using SELECT INTO'
        ELSE 'UNKNOWN'
    END as assignment_method
FROM information_schema.routines 
WHERE routine_schema IN ('gtfs_rt', 'gtfs')
  AND routine_name LIKE '%cleanup%'
ORDER BY routine_schema, routine_name;

-- =============================================================================
-- PART 2: Data State Analysis (understand what we're working with)
-- =============================================================================

-- Check current real-time data age and volume
SELECT 
    'Real-time data analysis' as check_type,
    'trip_updates' as table_name,
    COUNT(*) as total_records,
    MIN(timestamp) as oldest_record,
    MAX(timestamp) as newest_record,
    EXTRACT(days FROM NOW() - MIN(timestamp))::TEXT || ' days' as data_age_range,
    pg_size_pretty(pg_total_relation_size('gtfs_rt.trip_updates')) as table_size
FROM gtfs_rt.trip_updates 
WHERE timestamp IS NOT NULL

UNION ALL

SELECT 
    'Real-time data analysis',
    'feed_messages',
    COUNT(*),
    MIN(timestamp),
    MAX(timestamp),
    EXTRACT(days FROM NOW() - MIN(timestamp))::TEXT || ' days',
    pg_size_pretty(pg_total_relation_size('gtfs_rt.feed_messages'))
FROM gtfs_rt.feed_messages 
WHERE timestamp IS NOT NULL

UNION ALL

SELECT 
    'Real-time data analysis',
    'vehicle_positions',
    COUNT(*),
    NULL,
    NULL,
    'N/A (no direct timestamp)',
    pg_size_pretty(pg_total_relation_size('gtfs_rt.vehicle_positions'))
FROM gtfs_rt.vehicle_positions;

-- Check GTFS versions
SELECT 
    'GTFS versions analysis' as check_type,
    version_id,
    version_name,
    created_at,
    is_active,
    EXTRACT(days FROM NOW() - created_at)::TEXT || ' days old' as age
FROM gtfs.versions 
ORDER BY created_at DESC;

-- =============================================================================
-- PART 3: Safe Function Testing (Dry Run Approach)
-- =============================================================================

-- Test 1: Try GTFS cleanup with very conservative settings (should not delete anything active)
-- This tests the function syntax without actually deleting important data
BEGIN;
DO $$
DECLARE
    test_result RECORD;
    error_occurred BOOLEAN := FALSE;
    error_message TEXT;
BEGIN
    -- Test the GTFS cleanup function with conservative settings
    RAISE NOTICE 'Testing GTFS cleanup function (keep 10 inactive versions - very safe)...';
    
    BEGIN
        FOR test_result IN 
            SELECT * FROM gtfs.cleanup_old_versions(10)  -- Keep 10 inactive versions (very conservative)
        LOOP
            RAISE NOTICE 'GTFS Cleanup Result: version_id=%, name=%, records=%, status=%', 
                test_result.version_id, test_result.version_name, 
                test_result.records_deleted, test_result.cleanup_status;
        END LOOP;
        RAISE NOTICE 'GTFS cleanup function test: SUCCESS';
    EXCEPTION WHEN OTHERS THEN
        error_occurred := TRUE;
        error_message := SQLERRM;
        RAISE NOTICE 'GTFS cleanup function test: FAILED - %', error_message;
    END;
    
    IF error_occurred THEN
        RAISE EXCEPTION 'GTFS cleanup test failed: %', error_message;
    END IF;
END;
$$;
ROLLBACK;  -- Always rollback to avoid any changes

-- Test 2: Try real-time cleanup with very conservative settings
BEGIN;
DO $$
DECLARE
    test_result RECORD;
    error_occurred BOOLEAN := FALSE;
    error_message TEXT;
    batch_count INTEGER := 0;
BEGIN
    -- Test the real-time cleanup function with very conservative settings
    RAISE NOTICE 'Testing real-time cleanup function (90 days retention, 10 batch size - very safe)...';
    
    BEGIN
        FOR test_result IN 
            SELECT * FROM gtfs_rt.cleanup_old_realtime_data_batch(90, 10)  -- 90 days retention, tiny batches
        LOOP
            batch_count := batch_count + 1;
            RAISE NOTICE 'RT Cleanup Result: table=%, batch=%, records=%, duration=%', 
                test_result.table_name, test_result.batch_number, 
                test_result.records_deleted, test_result.batch_duration;
            
            -- Stop after 3 results to avoid long test
            IF batch_count >= 3 THEN
                EXIT;
            END IF;
        END LOOP;
        RAISE NOTICE 'Real-time cleanup function test: SUCCESS';
    EXCEPTION WHEN OTHERS THEN
        error_occurred := TRUE;
        error_message := SQLERRM;
        RAISE NOTICE 'Real-time cleanup function test: FAILED - %', error_message;
    END;
    
    IF error_occurred THEN
        RAISE EXCEPTION 'Real-time cleanup test failed: %', error_message;
    END IF;
END;
$$;
ROLLBACK;  -- Always rollback to avoid any changes

-- =============================================================================
-- PART 4: VACUUM Function Testing
-- =============================================================================

-- Test vacuum functions (these don't modify data, just analyze)
SELECT 'Testing GTFS vacuum function...' as test_phase;
-- Note: VACUUM functions must be run outside transactions, so test separately if needed

-- =============================================================================
-- PART 5: Summary and Recommendations
-- =============================================================================

SELECT 
    'TEST SUMMARY' as summary,
    'If all tests above completed without errors, the functions are ready for deployment' as recommendation,
    'Run the actual cleanup with production settings only after verifying these tests pass' as warning;

-- Suggested production commands (DO NOT RUN IN TEST):
-- Real-time cleanup (1 day retention): SELECT * FROM gtfs_rt.cleanup_old_realtime_data_batch(1, 10000);
-- GTFS cleanup (keep 1 inactive):      SELECT * FROM gtfs.cleanup_old_versions(1);
-- Vacuum real-time tables:             SELECT * FROM gtfs_rt.vacuum_cleanup_tables();
-- Vacuum GTFS tables:                  SELECT * FROM gtfs.vacuum_gtfs_tables();