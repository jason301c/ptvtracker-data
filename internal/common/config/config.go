package config

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	Database   DatabaseConfig
	GTFSStatic GTFSStaticConfig
	Logging    LoggingConfig
}

type DatabaseConfig struct {
	ConnectionString string
}

// GTFS_STATIC_URL (required)
// GTFS_STATIC_CHECK_INTERVAL (optional, default 30m)
// GTFS_STATIC_DOWNLOAD_DIR (optional, default /tmp/gtfs-static)
type GTFSStaticConfig struct {
	URL           string
	CheckInterval time.Duration
	DownloadDir   string
}

type LoggingConfig struct {
	Level    string
	FilePath string
}

func Load() (*Config, error) {
	cfg := &Config{
		Database: DatabaseConfig{
			ConnectionString: getEnvMultiple([]string{
				"DATABASE_URL",
				"POSTGRES_URL",
				"POSTGRESQL_URL",
				"PG_CONNECTION_STRING",
			}, ""),
		},
		GTFSStatic: GTFSStaticConfig{
			URL:           getEnv("GTFS_STATIC_URL", ""),
			CheckInterval: getDurationEnv("GTFS_STATIC_CHECK_INTERVAL", 30*time.Minute),
			DownloadDir:   getEnv("GTFS_STATIC_DOWNLOAD_DIR", "/tmp/gtfs-static"),
		},
		Logging: LoggingConfig{
			Level:    getEnv("LOG_LEVEL", "info"),
			FilePath: getEnv("LOG_FILE", "ptvtracker.log"),
		},
	}

	if cfg.GTFSStatic.URL == "" {
		return nil, fmt.Errorf("GTFS_STATIC_URL environment variable is required")
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
