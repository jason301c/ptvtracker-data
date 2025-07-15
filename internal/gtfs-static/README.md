# GTFS-Static Module

This module handles the downloading, parsing, and importing of static GTFS (General Transit Feed Specification) data from the Victorian data portal.

## Components

### Scraper (`/scraper`)
Monitors the Victorian data portal for updates to GTFS datasets.

- **metadata_fetcher.go**: Fetches dataset metadata from the API
- **downloader.go**: Downloads GTFS ZIP files with progress tracking
- **scheduler.go**: Orchestrates the checking and import process
- **interfaces.go**: Defines contracts for extensibility

### Parser (`/parser`)
Parses GTFS ZIP files containing CSV data.

- **parser.go**: Handles all GTFS file types (stops, routes, trips, etc.)
- Processes files in dependency order for referential integrity
- Streaming parser for memory efficiency

### Importer (`/importer`)
Imports parsed data into PostgreSQL.

- **importer.go**: Batch imports with transaction support
- Uses prepared statements for performance
- Handles blue-green deployment versioning

## Data Flow

1. **Check for Updates**
   - Scheduler periodically checks the Victorian data portal API
   - Compares last modified timestamp with active database version

2. **Download**
   - If newer data exists, downloads the GTFS ZIP file
   - Uses temporary files to ensure atomic downloads

3. **Parse**
   - Extracts and parses CSV files from the ZIP
   - Validates data according to GTFS specification

4. **Import**
   - Creates a new database version
   - Imports all data within a transaction
   - On success, activates the new version

## Blue-Green Deployment

The system uses database versioning for zero-downtime updates:

```sql
-- Multiple versions can exist
versions: v1 (inactive), v2 (active), v3 (importing)

-- Only one version is active at a time
UPDATE versions SET is_active = true WHERE version_id = 3;
```

## Configuration

Configure via environment variables:

```bash
# Check interval
GTFS_STATIC_CHECK_INTERVAL=30m

# Download directory
GTFS_STATIC_DOWNLOAD_DIR=/tmp/gtfs-static
```

## Supported GTFS Files

- ✅ agency.txt
- ✅ stops.txt
- ✅ routes.txt
- ✅ trips.txt
- ✅ stop_times.txt
- ✅ calendar.txt
- ✅ calendar_dates.txt
- ✅ shapes.txt
- ✅ levels.txt
- ✅ pathways.txt
- ✅ transfers.txt

## Error Handling

The module implements comprehensive error handling:

- Network failures: Automatic retry with exponential backoff
- Parsing errors: Logged and skipped, import continues
- Database errors: Full rollback, version remains inactive
- File corruption: Detected via checksums, download retried

## Performance Considerations

- **Batch Imports**: Default batch size of 1000 records
- **Streaming**: Files are parsed in streams, not loaded into memory
- **Indexes**: Created after import for faster loading
- **Transactions**: All-or-nothing imports ensure consistency

## Monitoring

The module provides detailed logging:

```
INFO: Checking for GTFS updates resource_id=33f95bee-1fad-4aeb-aa4e-0bc4f2ff0d85
INFO: New version detected last_modified=2024-01-15T10:30:00Z
INFO: Download progress progress_percent=45.2 bytes_downloaded=45234567
INFO: File parsed name=stop_times.txt records=1234567
INFO: Import completed successfully version_id=5
```

## Extending

To add a new GTFS source:

1. Add to `transport_sources` table
2. Configure in `config.go`:
   ```go
   Sources: []GTFSStaticSource{
       {
           Name:       "regional",
           SourceID:   2,
           ResourceID: "new-resource-id",
           Enabled:    true,
       },
   }
   ```

The system will automatically handle multiple sources concurrently.