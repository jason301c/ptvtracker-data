# PTV Tracker Data

A comprehensive data ingestion and processing system for Melbourne's Public Transport Victoria (PTV) data, supporting both GTFS-Static and GTFS-Realtime formats.

## Overview

This project provides a robust solution for:
- **GTFS-Static**: Downloading and importing static transit data (routes, stops, schedules)
- **GTFS-Realtime**: Processing real-time transit updates (vehicle positions, trip updates, service alerts)

## Architecture

The system is built with clear separation between static and real-time components:

```
┌─────────────────────┐     ┌──────────────────────┐
│   GTFS-Static       │     │   GTFS-Realtime      │
│                     │     │                      │
│  ┌───────────────┐  │     │  ┌────────────────┐  │
│  │   Scraper     │  │     │  │   Consumer     │  │
│  └───────┬───────┘  │     │  └────────┬───────┘  │
│          │          │     │           │          │
│  ┌───────▼───────┐  │     │  ┌───────▼────────┐  │
│  │   Parser      │  │     │  │   Parser       │  │
│  └───────┬───────┘  │     │  └────────┬───────┘  │
│          │          │     │           │          │
│  ┌───────▼───────┐  │     │  ┌───────▼────────┐  │
│  │   Importer    │  │     │  │   Processor    │  │
│  └───────┬───────┘  │     │  └────────┬───────┘  │
└──────────┼──────────┘     └───────────┼──────────┘
           │                            │
           └────────────┬───────────────┘
                        │
                 ┌──────▼──────┐
                 │  PostgreSQL  │
                 │              │
                 │ ┌──────────┐ │
                 │ │  Static  │ │
                 │ │  Tables  │ │
                 │ └──────────┘ │
                 │              │
                 │ ┌──────────┐ │
                 │ │ Realtime │ │
                 │ │  Tables  │ │
                 │ └──────────┘ │
                 └──────────────┘
```

## Features

### GTFS-Static
- Continuous monitoring of Victorian data portal for updates
- Blue-green deployment for zero-downtime data updates
- Comprehensive GTFS specification support
- Batch importing with transaction support
- Progress tracking and detailed logging

### GTFS-Realtime (Coming Soon)
- Real-time feed consumption
- Protocol buffer parsing
- Vehicle position tracking
- Trip update processing
- Service alert handling

## Installation

### Prerequisites
- Go 1.21 or later
- PostgreSQL 12 or later
- Network access to Victorian data portal

### Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/ptvtracker-data.git
cd ptvtracker-data
```

2. Install dependencies:
```bash
go mod download
```

3. Set up the database:
```bash
# Create database
createdb ptvtracker

# Apply migrations
psql -d ptvtracker -f sql/migrations/gtfs_static/001_tables.sql
psql -d ptvtracker -f sql/migrations/gtfs_static/002_indexes.sql
psql -d ptvtracker -f sql/migrations/gtfs_static/003_views.sql
psql -d ptvtracker -f sql/migrations/gtfs_static/004_functions.sql
```

4. Configure environment:
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. Run the application:
```bash
go run cmd/ptvtracker/main.go
```

## Configuration

Environment variables control the application behavior:

### Database
- `DB_HOST`: PostgreSQL host (default: localhost)
- `DB_PORT`: PostgreSQL port (default: 5432)
- `DB_USER`: Database user (default: postgres)
- `DB_PASSWORD`: Database password
- `DB_NAME`: Database name (default: ptvtracker)

### GTFS-Static
- `GTFS_STATIC_CHECK_INTERVAL`: How often to check for updates (default: 30m)
- `GTFS_STATIC_DOWNLOAD_DIR`: Temporary directory for downloads (default: /tmp/gtfs-static)

### GTFS-Realtime
- `GTFS_RT_POLLING_INTERVAL`: How often to poll real-time feeds (default: 30s)

### Logging
- `LOG_LEVEL`: Logging level (default: info)
- `LOG_FILE`: Log file path (default: ptvtracker.log)

## Project Structure

See [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) for detailed directory layout.

## Development

### Running Tests
```bash
go test ./...
```

### Building
```bash
go build -o ptvtracker cmd/ptvtracker/main.go
```

### Adding New Data Sources

1. Add the source to the `transport_sources` table
2. Configure in `internal/common/config/config.go`
3. The system will automatically handle the new source

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Public Transport Victoria for providing GTFS data
- Victorian Government Data Directory for hosting the datasets