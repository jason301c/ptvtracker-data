package scraper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ptvtracker-data/internal/common/db"
	"github.com/ptvtracker-data/internal/common/logger"
	"github.com/ptvtracker-data/internal/gtfs-static/importer"
)

type GTFSScheduler struct {
	config          Config
	metadataFetcher MetadataFetcher
	versionChecker  *db.VersionChecker
	downloader      Downloader
	database        *db.DB
	logger          logger.Logger

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool
}

type Config struct {
	ResourceID    string
	CheckInterval time.Duration
	DownloadDir   string
	SourceID      int
	SourceName    string
}

func NewScheduler(
	config Config,
	database *db.DB,
	logger logger.Logger,
) *GTFSScheduler {
	return &GTFSScheduler{
		config:          config,
		metadataFetcher: NewHTTPMetadataFetcher(logger),
		versionChecker:  db.NewVersionChecker(database),
		downloader:      NewHTTPDownloader(logger),
		database:        database,
		logger:          logger,
	}
}

func (s *GTFSScheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("scheduler already running")
	}

	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.running = true
	s.mu.Unlock()

	s.logger.Info("Starting GTFS scheduler",
		"resource_id", s.config.ResourceID,
		"check_interval", s.config.CheckInterval,
		"source", s.config.SourceName)

	// Initial check
	if err := s.checkAndUpdate(ctx); err != nil {
		s.logger.Error("Initial check failed", "error", err)
	}

	// Set up ticker for periodic checks
	ticker := time.NewTicker(s.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Scheduler stopped")
			return nil
		case <-ticker.C:
			if err := s.checkAndUpdate(ctx); err != nil {
				s.logger.Error("Scheduled check failed", "error", err)
			}
		}
	}
}

func (s *GTFSScheduler) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return fmt.Errorf("scheduler not running")
	}

	if s.cancel != nil {
		s.cancel()
	}

	s.running = false
	return nil
}

func (s *GTFSScheduler) checkAndUpdate(ctx context.Context) error {
	s.logger.Debug("Checking for GTFS updates", "resource_id", s.config.ResourceID)

	// Fetch metadata
	metadata, err := s.metadataFetcher.FetchMetadata(ctx, s.config.ResourceID)
	if err != nil {
		return fmt.Errorf("fetching metadata: %w", err)
	}

	// Check if we have a newer version
	hasNewer, err := s.versionChecker.HasNewerVersion(ctx, metadata.LastModified.Time)
	if err != nil {
		return fmt.Errorf("checking version: %w", err)
	}

	if !hasNewer {
		s.logger.Debug("No new version available")
		return nil
	}

	s.logger.Info("New version detected, starting import process",
		"last_modified", metadata.LastModified.Time)

	// Download the file
	downloadPath := filepath.Join(
		s.config.DownloadDir,
		fmt.Sprintf("gtfs_%s_%s.zip",
			s.config.SourceName,
			metadata.LastModified.Time.Format("20060102_150405")),
	)

	if err := s.downloader.Download(ctx, metadata.URL, downloadPath); err != nil {
		return fmt.Errorf("downloading file: %w", err)
	}
	defer os.Remove(downloadPath) // Clean up after import

	// Create new version
	versionName := fmt.Sprintf("%s_%s",
		s.config.SourceName,
		metadata.LastModified.Time.Format("2006-01-02_15:04:05"))

	versionID, err := s.versionChecker.CreateNewVersion(
		ctx,
		versionName,
		metadata.URL,
		metadata.LastModified.Time,
	)
	if err != nil {
		return fmt.Errorf("creating version: %w", err)
	}

	// Import the data
	imp := importer.NewImporter(s.database, s.config.SourceID, versionID)
	if err := imp.Import(ctx, downloadPath); err != nil {
		s.logger.Error("Import failed, version will remain inactive",
			"version_id", versionID,
			"error", err)
		return fmt.Errorf("importing data: %w", err)
	}

	// Activate the new version
	if err := s.versionChecker.ActivateVersion(ctx, versionID); err != nil {
		return fmt.Errorf("activating version: %w", err)
	}

	s.logger.Info("Successfully imported and activated new GTFS data",
		"version_id", versionID,
		"version_name", versionName)

	return nil
}
