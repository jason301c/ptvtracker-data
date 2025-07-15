package scraper

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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

// Config for GTFSScheduler: single-source, URL-based
type Config struct {
	URL           string
	CheckInterval time.Duration
	DownloadDir   string
}

func NewScheduler(
	config Config,
	database *db.DB,
	logger logger.Logger,
	metadataFetcher MetadataFetcher, // Use the interface
	downloader Downloader, // Use the interface
) *GTFSScheduler {
	return &GTFSScheduler{
		config:          config,
		metadataFetcher: metadataFetcher,
		versionChecker:  db.NewVersionChecker(database),
		downloader:      downloader,
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
		"url", s.config.URL,
		"check_interval", s.config.CheckInterval)

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
	s.logger.Debug("Checking for GTFS updates", "url", s.config.URL)

	// Fetch metadata using URL
	metadata, err := s.metadataFetcher.FetchMetadata(ctx, s.config.URL)
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
		fmt.Sprintf("gtfs_%s.zip",
			metadata.LastModified.Time.Format("20060102_150405")),
	)

	if err := s.downloader.Download(ctx, metadata.URL, downloadPath); err != nil {
		return fmt.Errorf("downloading file: %w", err)
	}
	defer os.Remove(downloadPath) // Clean up after import

	// Create new version
	versionName := fmt.Sprintf("gtfs_%s",
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

	// Create a temporary directory for nested zips
	tempExtractDir, err := os.MkdirTemp(s.config.DownloadDir, "gtfs-extract-*")
	if err != nil {
		return fmt.Errorf("creating temp extraction directory: %w", err)
	}
	defer os.RemoveAll(tempExtractDir)

	s.logger.Info("Extracting and importing from master zip", "path", downloadPath)

	// Open the master zip file
	zipReader, err := zip.OpenReader(downloadPath)
	if err != nil {
		return fmt.Errorf("opening master zip file: %w", err)
	}
	defer zipReader.Close()

	// Regex to find nested zips, e.g., "1/google_transit.zip"
	nestedZipRegex := regexp.MustCompile(`^(\d+)/google_transit\.zip$`)

	// Loop through all files in the master zip to find and import sources
	for _, file := range zipReader.File {
		if file.FileInfo().IsDir() {
			continue
		}

		matches := nestedZipRegex.FindStringSubmatch(file.Name)
		if len(matches) < 2 {
			continue
		}

		sourceID, err := strconv.Atoi(matches[1])
		if err != nil {
			s.logger.Warn("Could not parse source ID from path, skipping", "path", file.Name, "error", err)
			continue
		}

		s.logger.Info("Found source to import", "source_id", sourceID, "file", file.Name)

		// Extract the nested zip to a temporary file
		nestedZipPath := filepath.Join(tempExtractDir, fmt.Sprintf("source_%d.zip", sourceID))

		nestedZipFile, err := os.Create(nestedZipPath)
		if err != nil {
			return fmt.Errorf("creating temp file for source %d: %w", sourceID, err)
		}

		rc, err := file.Open()
		if err != nil {
			nestedZipFile.Close()
			return fmt.Errorf("opening nested zip for source %d: %w", sourceID, err)
		}

		_, err = io.Copy(nestedZipFile, rc)
		rc.Close()
		nestedZipFile.Close()
		if err != nil {
			return fmt.Errorf("extracting nested zip for source %d: %w", sourceID, err)
		}

		// Import the data for this source
		s.logger.Info("Starting import for source", "source_id", sourceID, "version_id", versionID)
		imp := importer.NewImporter(s.database, sourceID, versionID)
		if err := imp.Import(ctx, nestedZipPath); err != nil {
			s.logger.Error("Import failed for source, version will remain inactive",
				"source_id", sourceID,
				"version_id", versionID,
				"error", err)
			return fmt.Errorf("importing data for source %d: %w", sourceID, err)
		}
		s.logger.Info("Successfully imported source", "source_id", sourceID)
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
