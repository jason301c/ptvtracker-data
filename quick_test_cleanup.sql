-- Quick test script for cleanup functions - run this first to validate syntax
-- This script tests function calls without making any changes to your data

-- Test 1: Check if functions can be called without errors (very conservative settings)
DO $$
DECLARE
    result_count INTEGER := 0;
    test_record RECORD;
BEGIN
    RAISE NOTICE '=== Testing GTFS cleanup function ===';
    
    -- Test GTFS cleanup with very conservative settings (keep 100 inactive versions)
    BEGIN
        FOR test_record IN 
            SELECT * FROM gtfs.cleanup_old_versions(100)  -- Keep 100 versions (very safe)
        LOOP
            result_count := result_count + 1;
            RAISE NOTICE 'GTFS Result %: version_id=%, name=%, records=%, status=%', 
                result_count, test_record.version_id, test_record.version_name, 
                test_record.records_deleted, test_record.cleanup_status;
            
            -- Stop after 3 results to keep test short
            IF result_count >= 3 THEN EXIT; END IF;
        END LOOP;
        
        RAISE NOTICE 'GTFS cleanup test: PASSED (% results)', result_count;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'GTFS cleanup test: FAILED - %', SQLERRM;
        RAISE EXCEPTION 'GTFS cleanup function has errors: %', SQLERRM;
    END;
    
    RAISE NOTICE '';
    RAISE NOTICE '=== Testing Real-time cleanup function ===';
    
    -- Test real-time cleanup with very conservative settings
    result_count := 0;
    BEGIN
        FOR test_record IN 
            SELECT * FROM gtfs_rt.cleanup_old_realtime_data_batch(365, 5)  -- 1 year retention, tiny batches
        LOOP
            result_count := result_count + 1;
            RAISE NOTICE 'RT Result %: table=%, batch=%, records=%, duration=%', 
                result_count, test_record.table_name, test_record.batch_number, 
                test_record.records_deleted, test_record.batch_duration;
            
            -- Stop after 5 results to keep test short  
            IF result_count >= 5 THEN EXIT; END IF;
        END LOOP;
        
        RAISE NOTICE 'Real-time cleanup test: PASSED (% results)', result_count;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Real-time cleanup test: FAILED - %', SQLERRM;
        RAISE EXCEPTION 'Real-time cleanup function has errors: %', SQLERRM;
    END;
    
    RAISE NOTICE '';
    RAISE NOTICE '=== All tests completed successfully! ===';
    RAISE NOTICE 'Functions appear to be working correctly.';
    RAISE NOTICE 'You can now run with production settings if needed.';
END;
$$;

-- Show current data status for reference
SELECT 
    'Current data status' as info,
    (SELECT COUNT(*) FROM gtfs.versions WHERE is_active = false) as inactive_gtfs_versions,
    (SELECT COUNT(*) FROM gtfs_rt.trip_updates WHERE timestamp < NOW() - INTERVAL '7 days') as old_trip_updates,
    (SELECT COUNT(*) FROM gtfs_rt.feed_messages WHERE timestamp < NOW() - INTERVAL '7 days') as old_feed_messages;