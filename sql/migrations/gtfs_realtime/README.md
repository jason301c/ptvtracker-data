# GTFS Real-time SQL Migrations

## Overview

These migrations set up the complete GTFS real-time data processing system with automatic notifications via PostgreSQL triggers.

## Migration Order

Apply these migrations in order:

1. **001_realtime_tables.sql** - Creates all GTFS-RT tables
2. **002_realtime_functions.sql** - Helper functions for real-time data
3. **003_realtime_indexes.sql** - Performance indexes
4. **004_improved_notifications.sql** - Cleanup of old functions
5. **005_consolidated_notifications.sql** - Complete notification system

## Key Features

### Notification System

All notifications are handled by database triggers. The Go backend only needs to insert data - no manual notification sending required.

**Notification Channels:**
- `stop_departures:{source_id}:{stop_id}` - Real-time departure updates
- `trip_updates:{source_id}:{trip_id}` - Trip-level updates
- `vehicle_positions:{source_id}:{vehicle_id}` - Vehicle location updates
- `vehicle_at_stop:{source_id}:{stop_id}` - Vehicle arrival notifications
- `service_alerts_stop:{source_id}:{stop_id}` - Stop-specific alerts
- `service_alerts_route:{source_id}:{route_id}` - Route-specific alerts

### Frontend Integration

The notification payload format matches exactly what the frontend expects:

```json
{
  "type": "update",
  "source_id": 1,
  "stop_id": "12345",
  "trip_id": "trip_123",
  "route_id": "route_456",
  "arrival_delay": 120,
  "departure_delay": 120,
  "arrival_time": 1234567890,
  "departure_time": 1234567890,
  "schedule_relationship": 0,
  "timestamp": "2025-01-21T10:00:00Z",
  "operation": "INSERT"
}
```

## Performance Considerations

- Indexes on `stop_id` for fast lookup
- Partial index on recent feed messages
- Triggers fire AFTER insert/update for data consistency
- Minimal payload size for efficient notification delivery

## Testing

To test notifications:

```sql
-- Listen for notifications
LISTEN "stop_departures:1:12345";

-- Insert test data (will trigger notification)
INSERT INTO gtfs_rt.feed_messages (...) VALUES (...);
INSERT INTO gtfs_rt.trip_updates (...) VALUES (...);
INSERT INTO gtfs_rt.stop_time_updates (...) VALUES (...);

-- You should see the notification in psql
```

## Maintenance

- No manual notification code needed in application
- Triggers handle all notification logic
- To disable notifications: `ALTER TABLE ... DISABLE TRIGGER ...`
- To re-enable: `ALTER TABLE ... ENABLE TRIGGER ...`