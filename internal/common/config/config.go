package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Database     DatabaseConfig
	GTFSStatic   GTFSStaticConfig
	GTFSRealtime GTFSRealtimeConfig
	Logging      LoggingConfig
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

type GTFSRealtimeConfig struct {
	APIKey           string
	PollingInterval  time.Duration
	RateLimitPerMin  int
	CacheExpiration  time.Duration
	Endpoints        []EndpointConfig
}

type EndpointConfig struct {
	Name     string
	URL      string
	FeedType string // "trip_updates", "vehicle_positions", "service_alerts"
	Source   string // "metrobus", "metrotrain", "tram"
}

type LoggingConfig struct {
	Level      string
	FilePath   string
	DiscordURL string
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
		GTFSRealtime: GTFSRealtimeConfig{
			APIKey:          getEnv("GTFS_RT_API_KEY", ""),
			PollingInterval: getDurationEnv("GTFS_RT_POLLING_INTERVAL", 30*time.Second),
			RateLimitPerMin: getIntEnv("GTFS_RT_RATE_LIMIT_PER_MIN", 25),
			CacheExpiration: getDurationEnv("GTFS_RT_CACHE_EXPIRATION", 30*time.Second),
			Endpoints:       getDefaultEndpoints(),
		},
		Logging: LoggingConfig{
			Level:      getEnv("LOG_LEVEL", "info"),
			FilePath:   getEnv("LOG_FILE", "ptvtracker.log"),
			DiscordURL: getEnv("DISCORD_WEBHOOK_URL", ""),
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

func getIntEnv(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getDefaultEndpoints() []EndpointConfig {
	return []EndpointConfig{
		{
			Name:     "metrobus_trip_updates",
			URL:      "https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrobus-tripupdates",
			FeedType: "trip_updates",
			Source:   "Metropolitan Bus",
		},
		{
			Name:     "metrotrain_service_alerts",
			URL:      "https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrotrain-servicealerts",
			FeedType: "service_alerts",
			Source:   "Metropolitan Train",
		},
		{
			Name:     "metrotrain_trip_updates",
			URL:      "https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrotrain-tripupdates",
			FeedType: "trip_updates",
			Source:   "Metropolitan Train",
		},
		{
			Name:     "metrotrain_vehicle_positions",
			URL:      "https://data-exchange-api.vicroads.vic.gov.au/opendata/v1/gtfsr/metrotrain-vehicleposition-updates",
			FeedType: "vehicle_positions",
			Source:   "Metropolitan Train",
		},
		{
			Name:     "tram_service_alerts",
			URL:      "https://data-exchange-api.vicroads.vic.gov.au/opendata/gtfsr/v1/tram/servicealert",
			FeedType: "service_alerts",
			Source:   "Metropolitan Tram",
		},
		{
			Name:     "tram_trip_updates",
			URL:      "https://data-exchange-api.vicroads.vic.gov.au/opendata/gtfsr/v1/tram/tripupdates",
			FeedType: "trip_updates",
			Source:   "Metropolitan Tram",
		},
		{
			Name:     "tram_vehicle_positions",
			URL:      "https://data-exchange-api.vicroads.vic.gov.au/opendata/gtfsr/v1/tram/vehicleposition",
			FeedType: "vehicle_positions",
			Source:   "Metropolitan Tram",
		},
	}
}
