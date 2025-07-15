package gtfs_realtime

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/ptvtracker-data/internal/common/config"
	"github.com/ptvtracker-data/internal/common/logger"
	"github.com/ptvtracker-data/internal/gtfs-realtime/consumer"
	"github.com/ptvtracker-data/internal/gtfs-realtime/processor"
)

type Manager struct {
	config    config.GTFSRealtimeConfig
	logger    logger.Logger
	consumer  *consumer.Consumer
	processor *processor.Processor
	db        *sql.DB
	mu        sync.RWMutex
	isRunning bool
	cancelFn  context.CancelFunc
}

func NewManager(cfg config.GTFSRealtimeConfig, db *sql.DB, log logger.Logger) *Manager {
	return &Manager{
		config:    cfg,
		logger:    log,
		db:        db,
		consumer:  consumer.NewConsumer(cfg, log),
		processor: processor.NewProcessor(db, log),
	}
}

func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.isRunning {
		return fmt.Errorf("GTFS-realtime manager is already running")
	}

	// Validate configuration
	if err := m.validateConfig(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Create cancellable context
	ctx, cancel := context.WithCancel(ctx)
	m.cancelFn = cancel

	// Start consumer
	if err := m.consumer.Start(ctx); err != nil {
		cancel()
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	// Start processor
	if err := m.processor.Start(ctx, m.consumer.FeedChannel()); err != nil {
		cancel()
		return fmt.Errorf("failed to start processor: %w", err)
	}

	m.isRunning = true
	m.logger.Info("GTFS-realtime manager started successfully")

	return nil
}

func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.isRunning {
		return
	}

	m.logger.Info("Stopping GTFS-realtime manager")

	// Cancel context to stop all goroutines
	if m.cancelFn != nil {
		m.cancelFn()
	}

	// Stop consumer
	m.consumer.Stop()

	m.isRunning = false
	m.logger.Info("GTFS-realtime manager stopped")
}

func (m *Manager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isRunning
}

func (m *Manager) validateConfig() error {
	if m.config.APIKey == "" {
		return fmt.Errorf("API key is required")
	}

	if len(m.config.Endpoints) == 0 {
		return fmt.Errorf("at least one endpoint must be configured")
	}

	for _, endpoint := range m.config.Endpoints {
		if endpoint.URL == "" {
			return fmt.Errorf("endpoint URL cannot be empty")
		}
		if endpoint.FeedType == "" {
			return fmt.Errorf("endpoint feed type cannot be empty")
		}
		if endpoint.Source == "" {
			return fmt.Errorf("endpoint source cannot be empty")
		}
	}

	if m.config.PollingInterval <= 0 {
		return fmt.Errorf("polling interval must be positive")
	}

	if m.config.RateLimitPerMin <= 0 {
		return fmt.Errorf("rate limit per minute must be positive")
	}

	return nil
}