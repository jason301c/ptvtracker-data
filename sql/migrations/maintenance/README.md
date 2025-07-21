# Database Maintenance Procedures

This directory contains database maintenance scripts and procedures for the PTV Tracker application.

## Overview

The PTV Tracker database accumulates large amounts of real-time transport data that needs regular cleanup to maintain optimal performance. The database currently contains:

- **82 GB** of real-time stop time updates (574M+ records)
- **4.7 GB** of trip updates (23M+ records) 
- **1.9 GB** of vehicle positions (4.3M+ records)
- **3.4 GB** of static GTFS data

## Key Issues Identified

1. **Real-time data accumulation**: Over 64M+ old real-time records (>1 day old)
2. **No automated cleanup**: Currently no cleanup jobs running
3. **Missing query monitoring**: `pg_stat_statements` extension not installed
4. **Large table growth**: Real-time tables growing rapidly without bounds

## Maintenance Scripts

### 001_cleanup_jobs.sql

Contains four main functions for database maintenance:

#### `gtfs_rt.cleanup_old_realtime_data(retention_days INTEGER)`
- **Purpose**: Removes real-time data older than specified days (default: 7 days)
- **Usage**: `SELECT * FROM gtfs_rt.cleanup_old_realtime_data(7);`
- **Frequency**: **Run weekly** (recommended)
- **Impact**: Can free 60+ GB of space based on current data

#### `public.get_database_health_summary()`
- **Purpose**: Returns database health metrics and recommendations
- **Usage**: `SELECT * FROM public.get_database_health_summary();`
- **Frequency**: **Run daily** for monitoring

#### `public.install_query_monitoring()`
- **Purpose**: Installs `pg_stat_statements` extension for query performance monitoring
- **Usage**: `SELECT public.install_query_monitoring();`
- **Frequency**: **Run once** (requires restart if successful)

#### `public.refresh_materialized_views()`
- **Purpose**: Refreshes all materialized views (tram stop groups, etc.)
- **Usage**: `SELECT * FROM public.refresh_materialized_views();`
- **Frequency**: **Run when GTFS data is updated**

#### `gtfs.cleanup_old_versions(keep_inactive_versions INTEGER)`
- **Purpose**: Removes old inactive GTFS versions, keeping only active + specified recent versions
- **Usage**: `SELECT * FROM gtfs.cleanup_old_versions(1);` (keeps 1 inactive version)
- **Frequency**: **Run after importing new GTFS data**
- **Impact**: Can free several GB per old version (each version ~6GB+ of static data)

#### `gtfs.list_versions_with_sizes()`
- **Purpose**: Lists all GTFS versions with record counts and estimated storage sizes
- **Usage**: `SELECT * FROM gtfs.list_versions_with_sizes();`
- **Frequency**: **Run before cleanup** to see what versions exist

## Recommended Maintenance Schedule

### Daily
```sql
-- Check database health
SELECT * FROM public.get_database_health_summary();
```

### Weekly  
```sql
-- Clean up old real-time data (7-day retention)
SELECT * FROM gtfs_rt.cleanup_old_realtime_data(7);
```

### When GTFS Data Updates
```sql
-- Refresh materialized views
SELECT * FROM public.refresh_materialized_views();

-- Check current GTFS versions and sizes
SELECT * FROM gtfs.list_versions_with_sizes();

-- Clean up old GTFS versions (keep only 1 inactive version as backup)
SELECT * FROM gtfs.cleanup_old_versions(1);
```

### One-time Setup
```sql
-- Install query monitoring
SELECT public.install_query_monitoring();
-- (May require PostgreSQL restart and superuser privileges)
```

## Automation Options

Consider setting up automated maintenance using:

1. **PostgreSQL cron extension** (`pg_cron`)
2. **System cron jobs** with `psql` commands
3. **Application-level scheduled tasks**

Example cron job (weekly cleanup):
```bash
# Weekly cleanup of old real-time data (Sundays at 2 AM)
0 2 * * 0 psql -d ptvtracker -c "SELECT * FROM gtfs_rt.cleanup_old_realtime_data(7);"
```

## Performance Monitoring

After installing `pg_stat_statements`, monitor slow queries with:

```sql
-- Top 10 slowest queries by total time
SELECT 
  query,
  calls,
  total_exec_time,
  mean_exec_time,
  rows
FROM pg_stat_statements 
ORDER BY total_exec_time DESC 
LIMIT 10;
```

## Safety Notes

- All cleanup functions include transaction safety
- Vacuum/analyze is automatically run after cleanup
- Foreign key constraints ensure referential integrity
- Functions return detailed results showing space freed
- Test cleanup functions on small datasets first

## Current Database Status

- **Autovacuum**: Working properly (frequent runs on real-time tables)
- **Query monitoring**: Not installed (needs `pg_stat_statements`)
- **Cleanup**: No automated cleanup (manual intervention needed)
- **Health**: Good overall, but accumulating old data