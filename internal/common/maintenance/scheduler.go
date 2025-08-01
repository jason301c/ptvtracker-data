package maintenance

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ptvtracker-data/internal/common/db"
	"github.com/ptvtracker-data/internal/common/logger"
)

// CleanupScheduler handles periodic maintenance tasks
type CleanupScheduler struct {
	maintenance         *Maintenance
	logger              logger.Logger
	db                  *db.DB
	config              SchedulerConfig
	isRunning           bool
	mu                  sync.RWMutex
	cancelFn            context.CancelFunc
	importLock          sync.RWMutex // Prevents cleanup during GTFS imports
	isImportInProgress  bool
}

// SchedulerConfig contains configuration for the cleanup scheduler
type SchedulerConfig struct {
	RealtimeCleanupInterval time.Duration // How often to clean real-time data
	StaticCleanupInterval   time.Duration // How often to clean old GTFS versions
	RealtimeRetentionDays   int           // Days to keep real-time data
	KeepInactiveVersions    int           // Number of inactive GTFS versions to keep
	BatchSize               int           // Records per batch for batched cleanup (default: 10000)
	UseBatchedCleanup       bool          // Whether to use batched cleanup (default: true)
}

// DefaultSchedulerConfig returns sensible defaults
func DefaultSchedulerConfig() SchedulerConfig {
	return SchedulerConfig{
		RealtimeCleanupInterval: 6 * time.Hour,  // Every 6 hours
		StaticCleanupInterval:   24 * time.Hour, // Daily
		RealtimeRetentionDays:   1,              // Keep 1 day of real-time data
		KeepInactiveVersions:    1,              // Keep 1 inactive version as backup
		BatchSize:               10000,          // 10K records per batch
		UseBatchedCleanup:       true,           // Use batched cleanup by default
	}
}

// NewCleanupScheduler creates a new cleanup scheduler
func NewCleanupScheduler(database *db.DB, logger logger.Logger, config SchedulerConfig) *CleanupScheduler {
	return &CleanupScheduler{
		maintenance: New(database, logger),
		logger:      logger,
		db:          database,
		config:      config,
	}
}

// Start begins the cleanup scheduling
func (s *CleanupScheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isRunning {
		return fmt.Errorf("cleanup scheduler is already running")
	}

	ctx, cancel := context.WithCancel(ctx)
	s.cancelFn = cancel
	s.isRunning = true

	s.logger.Info("Starting cleanup scheduler",
		"realtime_interval", s.config.RealtimeCleanupInterval,
		"static_interval", s.config.StaticCleanupInterval,
		"batch_size", s.config.BatchSize,
		"use_batched_cleanup", s.config.UseBatchedCleanup)

	// Start real-time cleanup goroutine
	go s.realtimeCleanupLoop(ctx)

	// Start static cleanup goroutine
	go s.staticCleanupLoop(ctx)

	return nil
}

// Stop stops the cleanup scheduler
func (s *CleanupScheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isRunning {
		return
	}

	s.logger.Info("Stopping cleanup scheduler")

	if s.cancelFn != nil {
		s.cancelFn()
	}

	s.isRunning = false
	s.logger.Info("Cleanup scheduler stopped")
}

// IsRunning returns whether the scheduler is active
func (s *CleanupScheduler) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isRunning
}

// LockForImport prevents cleanup operations during GTFS imports
func (s *CleanupScheduler) LockForImport() {
	s.importLock.Lock()
	s.isImportInProgress = true
	s.logger.Info("Cleanup operations locked for GTFS import")
}

// UnlockAfterImport allows cleanup operations to resume after GTFS import
func (s *CleanupScheduler) UnlockAfterImport() {
	s.isImportInProgress = false
	s.importLock.Unlock()
	s.logger.Info("Cleanup operations unlocked after GTFS import")
}

// canPerformCleanup checks if cleanup operations are allowed
func (s *CleanupScheduler) canPerformCleanup() bool {
	s.importLock.RLock()
	defer s.importLock.RUnlock()
	return !s.isImportInProgress
}

// realtimeCleanupLoop runs periodic real-time data cleanup
func (s *CleanupScheduler) realtimeCleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(s.config.RealtimeCleanupInterval)
	defer ticker.Stop()

	// Run initial cleanup after a short delay
	initialDelay := time.NewTimer(1 * time.Minute)
	defer initialDelay.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Real-time cleanup loop stopping")
			return

		case <-initialDelay.C:
			s.performRealtimeCleanup(ctx)

		case <-ticker.C:
			s.performRealtimeCleanup(ctx)
		}
	}
}

// staticCleanupLoop runs periodic GTFS version cleanup
func (s *CleanupScheduler) staticCleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(s.config.StaticCleanupInterval)
	defer ticker.Stop()

	// Run initial cleanup after 5 minutes (after potential startup import)
	initialDelay := time.NewTimer(5 * time.Minute)
	defer initialDelay.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Static cleanup loop stopping")
			return

		case <-initialDelay.C:
			s.performStaticCleanup(ctx)

		case <-ticker.C:
			s.performStaticCleanup(ctx)
		}
	}
}

// performRealtimeCleanup executes real-time data cleanup
func (s *CleanupScheduler) performRealtimeCleanup(ctx context.Context) {
	if !s.canPerformCleanup() {
		s.logger.Debug("Skipping real-time cleanup - GTFS import in progress")
		return
	}

	s.logger.Info("Starting scheduled real-time data cleanup",
		"retention_days", s.config.RealtimeRetentionDays,
		"use_batched", s.config.UseBatchedCleanup,
		"batch_size", s.config.BatchSize)
	
	start := time.Now()
	var err error
	
	if s.config.UseBatchedCleanup {
		err = s.maintenance.CleanupOldRealtimeDataBatched(ctx, s.config.RealtimeRetentionDays, s.config.BatchSize)
	} else {
		err = s.maintenance.CleanupOldRealtimeData(ctx, s.config.RealtimeRetentionDays)
	}
	
	duration := time.Since(start)

	if err != nil {
		s.logger.Error("Real-time cleanup failed", 
			"error", err, 
			"duration", duration,
			"method", func() string {
				if s.config.UseBatchedCleanup {
					return "batched"
				}
				return "legacy"
			}())
	} else {
		s.logger.Info("Real-time cleanup completed successfully", 
			"duration", duration,
			"method", func() string {
				if s.config.UseBatchedCleanup {
					return "batched"
				}
				return "legacy"
			}())
	}
}

// performStaticCleanup executes GTFS version cleanup
func (s *CleanupScheduler) performStaticCleanup(ctx context.Context) {
	if !s.canPerformCleanup() {
		s.logger.Debug("Skipping static cleanup - GTFS import in progress")
		return
	}

	s.logger.Info("Starting scheduled GTFS version cleanup",
		"keep_inactive_versions", s.config.KeepInactiveVersions)
	
	start := time.Now()
	results, err := s.maintenance.CleanupOldGTFSVersions(ctx, s.config.KeepInactiveVersions)
	duration := time.Since(start)

	if err != nil {
		s.logger.Error("GTFS version cleanup failed", 
			"error", err, 
			"duration", duration)
	} else {
		s.logger.Info("GTFS version cleanup completed successfully", 
			"duration", duration,
			"versions_processed", len(results))
	}
}

// TriggerRealtimeCleanup manually triggers real-time cleanup (for testing/manual use)
func (s *CleanupScheduler) TriggerRealtimeCleanup(ctx context.Context) error {
	if !s.canPerformCleanup() {
		return fmt.Errorf("cannot perform cleanup - GTFS import in progress")
	}

	s.logger.Info("Manual real-time cleanup triggered",
		"retention_days", s.config.RealtimeRetentionDays,
		"use_batched", s.config.UseBatchedCleanup)
	
	if s.config.UseBatchedCleanup {
		return s.maintenance.CleanupOldRealtimeDataBatched(ctx, s.config.RealtimeRetentionDays, s.config.BatchSize)
	}
	return s.maintenance.CleanupOldRealtimeData(ctx, s.config.RealtimeRetentionDays)
}

// TriggerStaticCleanup manually triggers GTFS version cleanup (for testing/manual use)  
func (s *CleanupScheduler) TriggerStaticCleanup(ctx context.Context) error {
	if !s.canPerformCleanup() {
		return fmt.Errorf("cannot perform cleanup - GTFS import in progress")
	}

	s.logger.Info("Manual GTFS version cleanup triggered",
		"keep_inactive_versions", s.config.KeepInactiveVersions)
	_, err := s.maintenance.CleanupOldGTFSVersions(ctx, s.config.KeepInactiveVersions)
	return err
}

// GetStatus returns the current status of the cleanup scheduler
func (s *CleanupScheduler) GetStatus() map[string]interface{} {
	s.mu.RLock()
	s.importLock.RLock()
	defer s.mu.RUnlock()
	defer s.importLock.RUnlock()

	return map[string]interface{}{
		"is_running":               s.isRunning,
		"is_import_in_progress":    s.isImportInProgress,
		"realtime_interval":        s.config.RealtimeCleanupInterval.String(),
		"static_interval":          s.config.StaticCleanupInterval.String(),
		"realtime_retention_days":  s.config.RealtimeRetentionDays,
		"keep_inactive_versions":   s.config.KeepInactiveVersions,
		"batch_size":               s.config.BatchSize,
		"use_batched_cleanup":      s.config.UseBatchedCleanup,
	}
}