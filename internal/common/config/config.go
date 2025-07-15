package config

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	Database     DatabaseConfig
	GTFSStatic   GTFSStaticConfig
	GTFSRealtime GTFSRealtimeConfig
	Logging      LoggingConfig
}

type DatabaseConfig struct {
	// PostgreSQL connection string
	// Format: postgres://user:password@host:port/dbname?sslmode=disable
	// or: postgresql://user:password@host:port/dbname?sslmode=disable
	ConnectionString string
}

// GTFSStaticConfig for static GTFS data scraping
type GTFSStaticConfig struct {
	CheckInterval time.Duration
	DownloadDir   string
	Sources       []GTFSStaticSource
}

type GTFSStaticSource struct {
	Name       string
	SourceID   int
	ResourceID string // Victorian data portal resource ID
	Enabled    bool
}

// GTFSRealtimeConfig for real-time GTFS data consumption
type GTFSRealtimeConfig struct {
	PollingInterval time.Duration
	Feeds           []GTFSRealtimeFeed
}

type GTFSRealtimeFeed struct {
	Name     string
	SourceID int
	URL      string
	APIKey   string // If required
	FeedType string // "vehicle_positions", "trip_updates", "service_alerts"
	Enabled  bool
}

type LoggingConfig struct {
	Level    string
	FilePath string
}

func Load() (*Config, error) {
	cfg := &Config{
		Database: DatabaseConfig{
			// Support multiple common environment variable names
			ConnectionString: getEnvMultiple([]string{
				"DATABASE_URL",         // Standard
				"POSTGRES_URL",         // Alternative
				"POSTGRESQL_URL",       // Alternative
				"PG_CONNECTION_STRING", // Alternative
			}, "postgres://jason:@localhost:5432/ptvtracker-data-test?sslmode=disable"),
		},
		GTFSStatic: GTFSStaticConfig{
			CheckInterval: getDurationEnv("GTFS_STATIC_CHECK_INTERVAL", 30*time.Minute),
			DownloadDir:   getEnv("GTFS_STATIC_DOWNLOAD_DIR", "/tmp/gtfs-static"),
			Sources: []GTFSStaticSource{
				{
					Name:       "metro",
					SourceID:   1,
					ResourceID: "33f95bee-1fad-4aeb-aa4e-0bc4f2ff0d85",
					Enabled:    true,
				},
			},
		},
		GTFSRealtime: GTFSRealtimeConfig{
			PollingInterval: getDurationEnv("GTFS_RT_POLLING_INTERVAL", 30*time.Second),
			Feeds:           []GTFSRealtimeFeed{
				// Placeholder for real-time feeds
				// Will be populated when implementing GTFS-RT
			},
		},
		Logging: LoggingConfig{
			Level:    getEnv("LOG_LEVEL", "info"),
			FilePath: getEnv("LOG_FILE", "ptvtracker.log"),
		},
	}

	return cfg, nil
}

// Validate checks if the database configuration is valid
func (c *DatabaseConfig) Validate() error {
	if c.ConnectionString == "" {
		return fmt.Errorf("database connection string is empty")
	}
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvMultiple tries multiple environment variable names and returns the first non-empty value
func getEnvMultiple(keys []string, defaultValue string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return defaultValue
}

func getDurationEnv(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
