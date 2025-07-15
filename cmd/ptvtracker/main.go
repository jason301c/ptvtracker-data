package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/ptvtracker-data/internal/common/config"
	"github.com/ptvtracker-data/internal/common/db"
	"github.com/ptvtracker-data/internal/common/logger"
	"github.com/ptvtracker-data/internal/gtfs-static/scraper"
	gtfs_realtime "github.com/ptvtracker-data/internal/gtfs-realtime"
)

func main() {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		// If .env doesn't exist, let's exit.
		panic("Failed to load .env file: " + err.Error())
	}

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		panic("Failed to load configuration: " + err.Error())
	}

	// Initialize logger with configured level and Discord support
	loggerConfig := logger.LoggerConfig{
		Level:           logger.ParseLogLevel(cfg.Logging.Level),
		Console:         true,
		File:            true,
		FilePath:        cfg.Logging.FilePath,
		MaxSizeMB:       10,
		MaxBackups:      5,
		MaxAgeDays:      30,
		Compress:        true,
		TimeFieldFormat: "2006-01-02T15:04:05Z07:00",
		DiscordURL:      cfg.Logging.DiscordURL,
	}
	logger.InitLogger(loggerConfig)
	
	log := logger.New(
		logger.ConsoleWriter(),
		logger.FileWriter(cfg.Logging.FilePath),
	)

	log.Info("PTV Tracker Data Service starting",
		"version", "1.0.0",
		"log_level", cfg.Logging.Level,
		"static_url", cfg.GTFSStatic.URL,
		"realtime_endpoints", len(cfg.GTFSRealtime.Endpoints),
	)

	// Validate database configuration
	if err := cfg.Database.Validate(); err != nil {
		log.Fatal("Invalid database configuration", "error", err)
	}

	// Connect to database
	database, err := db.New(cfg.Database.ConnectionString, log)
	if err != nil {
		log.Fatal("Failed to connect to database", "error", err)
	}
	defer database.Close()

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	// Start GTFS-Static scheduler (single source)
	log.Info("Starting GTFS-Static scheduler", "url", cfg.GTFSStatic.URL)
	schedulerCfg := scraper.Config{
		URL:           cfg.GTFSStatic.URL,
		CheckInterval: cfg.GTFSStatic.CheckInterval,
		DownloadDir:   cfg.GTFSStatic.DownloadDir,
	}
	// Create dependencies for scheduler
	metadataFetcher := scraper.NewHTTPMetadataFetcher(log)
	downloader := scraper.NewHTTPDownloader(log)
	scheduler := scraper.NewScheduler(schedulerCfg, database, log, metadataFetcher, downloader)
	wg.Add(1)
	go func(s *scraper.GTFSScheduler) {
		defer wg.Done()
		if err := s.Start(ctx); err != nil {
			log.Error("GTFS-Static scheduler error", "error", err)
		}
	}(scheduler)

	// Start GTFS-Realtime manager (if API key is provided)
	if cfg.GTFSRealtime.APIKey != "" {
		log.Info("Starting GTFS-Realtime manager", "endpoints", len(cfg.GTFSRealtime.Endpoints))
		rtManager := gtfs_realtime.NewManager(cfg.GTFSRealtime, database.DB(), log)
		wg.Add(1)
		go func(m *gtfs_realtime.Manager) {
			defer wg.Done()
			if err := m.Start(ctx); err != nil {
				log.Error("GTFS-Realtime manager error", "error", err)
			}
		}(rtManager)
	} else {
		log.Info("GTFS-Realtime manager disabled (no API key provided)")
	}

	// Wait for shutdown signal
	<-sigChan
	log.Info("Shutdown signal received")

	// Cancel context to stop all schedulers
	cancel()

	// Wait for all schedulers to finish
	wg.Wait()

	log.Info("PTV Tracker Data Service stopped")
}
