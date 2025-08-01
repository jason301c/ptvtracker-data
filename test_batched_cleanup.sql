-- Test script for the new batched cleanup functionality
-- Run this to verify the new functions are working correctly

-- First, check if the new function exists
SELECT 
    routine_name,
    routine_type,
    data_type as return_type,
    routine_definition IS NOT NULL as has_definition
FROM information_schema.routines 
WHERE routine_schema = 'gtfs_rt' 
  AND routine_name LIKE '%cleanup%'
ORDER BY routine_name;

-- Test the optimized batched cleanup function 
-- Start with checking what data exists first
SELECT 
    'Data age check' as test,
    EXTRACT(days FROM NOW() - MIN(timestamp))::TEXT || ' days old' as oldest_data,
    EXTRACT(days FROM NOW() - MAX(timestamp))::TEXT || ' days old' as newest_data,
    COUNT(*) as total_records
FROM gtfs_rt.trip_updates 
WHERE timestamp IS NOT NULL;

-- Test a very conservative cleanup (30 days retention, small batch)
-- This should only clean very old data if any exists
SELECT 
    table_name,
    batch_number,
    records_deleted,
    size_freed,
    batch_duration
FROM gtfs_rt.cleanup_old_realtime_data_batch(
    30,   -- retention_days: keep last 30 days (very conservative)
    100   -- batch_size: small batches
)
WHERE batch_number <= 2  -- Only show first 2 batches per table
ORDER BY table_name, batch_number;

-- After cleanup, you can run VACUUM ANALYZE separately (outside transaction):
-- SELECT * FROM gtfs_rt.vacuum_cleanup_tables();

-- Check current data age to understand what would be cleaned
SELECT 
    'Current oldest data age' as metric,
    EXTRACT(days FROM NOW() - MIN(timestamp))::TEXT || ' days' as value
FROM gtfs_rt.feed_messages 
WHERE timestamp IS NOT NULL;

-- Check table sizes before potential cleanup
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_total_relation_size(schemaname||'.'||tablename) as total_bytes
FROM pg_tables 
WHERE schemaname = 'gtfs_rt'
  AND tablename IN ('vehicle_positions', 'stop_time_updates', 'trip_updates', 'alerts', 'feed_messages')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;