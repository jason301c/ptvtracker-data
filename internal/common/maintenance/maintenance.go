package maintenance

import (
	"context"
	"fmt"

	"github.com/ptvtracker-data/internal/common/db"
	"github.com/ptvtracker-data/internal/common/logger"
)

// RealtimeDataType represents the type of realtime data
type RealtimeDataType string

const (
	VehiclePositions RealtimeDataType = "vehicle_positions"
	TripUpdates      RealtimeDataType = "trip_updates"
	StopTimeUpdates  RealtimeDataType = "stop_time_updates"
	Alerts           RealtimeDataType = "alerts"
	FeedMessages     RealtimeDataType = "feed_messages"
)

// CleanupResult represents the result of a cleanup operation
type CleanupResult struct {
	DataType        RealtimeDataType
	SourceID        int
	RecordsDeleted  int64
	Success         bool
	Error           string
}

// VersionCleanupResult represents the result of version cleanup
type VersionCleanupResult struct {
	VersionID      *int    `json:"version_id"`
	VersionName    string  `json:"version_name"`
	RecordsDeleted *int64  `json:"records_deleted"`
	SizeFreed      string  `json:"size_freed"`
	CleanupStatus  string  `json:"cleanup_status"`
}

// Maintenance handles database cleanup and maintenance operations
type Maintenance struct {
	db     *db.DB
	logger logger.Logger
}

// New creates a new Maintenance instance
func New(database *db.DB, logger logger.Logger) *Maintenance {
	return &Maintenance{
		db:     database,
		logger: logger,
	}
}

// CleanupOldGTFSVersions removes old inactive GTFS versions, keeping only
// the active version and a specified number of recent inactive versions
func (m *Maintenance) CleanupOldGTFSVersions(ctx context.Context, keepInactiveVersions int) ([]VersionCleanupResult, error) {
	m.logger.Info("Starting cleanup of old GTFS versions", "keep_inactive_versions", keepInactiveVersions)

	query := `SELECT * FROM gtfs.cleanup_old_versions($1)`
	rows, err := m.db.DB().QueryContext(ctx, query, keepInactiveVersions)
	if err != nil {
		return nil, fmt.Errorf("executing cleanup_old_versions: %w", err)
	}
	defer rows.Close()

	var results []VersionCleanupResult
	for rows.Next() {
		var result VersionCleanupResult
		err := rows.Scan(
			&result.VersionID,
			&result.VersionName,
			&result.RecordsDeleted,
			&result.SizeFreed,
			&result.CleanupStatus,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning cleanup result: %w", err)
		}
		results = append(results, result)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating cleanup results: %w", err)
	}

	// Log results
	for _, result := range results {
		if result.VersionID != nil {
			m.logger.Info("Cleaned up GTFS version",
				"version_id", *result.VersionID,
				"version_name", result.VersionName,
				"records_deleted", *result.RecordsDeleted,
				"status", result.CleanupStatus)
		} else {
			m.logger.Info("GTFS cleanup summary",
				"size_freed", result.SizeFreed,
				"status", result.CleanupStatus)
		}
	}

	// Run VACUUM ANALYZE separately (outside transaction) if any versions were cleaned
	if len(results) > 0 {
		if err := m.VacuumGTFSTables(ctx); err != nil {
			m.logger.Warn("Failed to vacuum GTFS tables after cleanup", "error", err)
			// Don't return error - cleanup was successful, vacuum is just optimization
		}
	}

	return results, nil
}

// BatchedCleanupResult represents the result of a batched cleanup operation
type BatchedCleanupResult struct {
	TableName      string
	BatchNumber    int
	RecordsDeleted int64
	SizeFreed      string
	BatchDuration  *string // Using pointer to handle NULL values
}

// CleanupOldRealtimeDataBatched removes real-time data older than specified days using batched processing
func (m *Maintenance) CleanupOldRealtimeDataBatched(ctx context.Context, retentionDays int, batchSize int) error {
	m.logger.Info("Starting batched cleanup of old realtime data", 
		"retention_days", retentionDays,
		"batch_size", batchSize)

	query := `SELECT * FROM gtfs_rt.cleanup_old_realtime_data_batch($1, $2)`
	rows, err := m.db.DB().QueryContext(ctx, query, retentionDays, batchSize)
	if err != nil {
		return fmt.Errorf("executing cleanup_old_realtime_data_batch: %w", err)
	}
	defer rows.Close()

	tableStats := make(map[string]struct {
		totalDeleted int64
		batchCount   int
		sizeFreed    string
	})

	for rows.Next() {
		var result BatchedCleanupResult
		
		err := rows.Scan(&result.TableName, &result.BatchNumber, &result.RecordsDeleted, &result.SizeFreed, &result.BatchDuration)
		if err != nil {
			return fmt.Errorf("scanning cleanup result: %w", err)
		}

		if result.BatchNumber == 0 {
			// Summary record for this table
			stats := tableStats[result.TableName]
			stats.sizeFreed = result.SizeFreed
			tableStats[result.TableName] = stats
			
			m.logger.Info("Completed cleanup for table",
				"table", result.TableName,
				"total_records_deleted", result.RecordsDeleted,
				"total_batches", stats.batchCount,
				"size_freed", result.SizeFreed)
		} else {
			// Individual batch record
			stats := tableStats[result.TableName]
			stats.totalDeleted += result.RecordsDeleted
			stats.batchCount++
			tableStats[result.TableName] = stats
			
			durationStr := "unknown"
			if result.BatchDuration != nil {
				durationStr = *result.BatchDuration
			}
			
			m.logger.Debug("Processed batch",
				"table", result.TableName,
				"batch", result.BatchNumber,
				"records_deleted", result.RecordsDeleted,
				"duration", durationStr)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating cleanup results: %w", err)
	}

	// Calculate total across all tables
	totalDeleted := int64(0)
	totalBatches := 0
	for _, stats := range tableStats {
		totalDeleted += stats.totalDeleted
		totalBatches += stats.batchCount
	}

	m.logger.Info("Batched realtime cleanup completed", 
		"total_records_deleted", totalDeleted,
		"total_batches", totalBatches,
		"tables_processed", len(tableStats))
	
	// Run VACUUM ANALYZE separately (outside transaction)
	if err := m.VacuumCleanupTables(ctx); err != nil {
		m.logger.Warn("Failed to vacuum tables after cleanup", "error", err)
		// Don't return error - cleanup was successful, vacuum is just optimization
	}
	
	return nil
}

// CleanupOldRealtimeData removes real-time data older than specified days (backwards compatibility)
// This now uses the batched approach internally
func (m *Maintenance) CleanupOldRealtimeData(ctx context.Context, retentionDays int) error {
	m.logger.Info("Starting cleanup of old realtime data (legacy method)", "retention_days", retentionDays)
	
	// Use batched cleanup with a reasonable batch size
	return m.CleanupOldRealtimeDataBatched(ctx, retentionDays, 5000)
}

// VacuumResult represents the result of a vacuum operation
type VacuumResult struct {
	TableName string
	Operation string
	Duration  *string // Using pointer to handle NULL values
	Status    string
}

// VacuumCleanupTables runs VACUUM ANALYZE on cleanup tables (must be called outside transaction)
func (m *Maintenance) VacuumCleanupTables(ctx context.Context) error {
	m.logger.Info("Starting VACUUM ANALYZE of cleanup tables")

	query := `SELECT * FROM gtfs_rt.vacuum_cleanup_tables()`
	rows, err := m.db.DB().QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("executing vacuum_cleanup_tables: %w", err)
	}
	defer rows.Close()

	successCount := 0
	totalTables := 0

	for rows.Next() {
		var result VacuumResult
		
		err := rows.Scan(&result.TableName, &result.Operation, &result.Duration, &result.Status)
		if err != nil {
			return fmt.Errorf("scanning vacuum result: %w", err)
		}

		totalTables++
		
		durationStr := "unknown"
		if result.Duration != nil {
			durationStr = *result.Duration
		}

		if result.Status == "SUCCESS" {
			successCount++
			m.logger.Info("Vacuumed table successfully",
				"table", result.TableName,
				"operation", result.Operation,
				"duration", durationStr)
		} else {
			m.logger.Error("Failed to vacuum table",
				"table", result.TableName,
				"operation", result.Operation,
				"duration", durationStr,
				"error", result.Status)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating vacuum results: %w", err)
	}

	m.logger.Info("VACUUM ANALYZE completed", 
		"successful_tables", successCount,
		"total_tables", totalTables)
	
	if successCount < totalTables {
		return fmt.Errorf("vacuum failed for %d out of %d tables", totalTables-successCount, totalTables)
	}
	
	return nil
}

// RefreshMaterializedViews refreshes all materialized views in the gtfs schema
func (m *Maintenance) RefreshMaterializedViews(ctx context.Context) error {
	m.logger.Info("Refreshing materialized views")

	query := `SELECT * FROM public.refresh_materialized_views()`
	rows, err := m.db.DB().QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("executing refresh_materialized_views: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var viewName, refreshStatus string
		var duration string // Using string for simplicity
		err := rows.Scan(&viewName, &refreshStatus, &duration)
		if err != nil {
			return fmt.Errorf("scanning refresh result: %w", err)
		}

		if refreshStatus == "SUCCESS" {
			m.logger.Info("Refreshed materialized view",
				"view", viewName,
				"duration", duration)
		} else {
			m.logger.Error("Failed to refresh materialized view",
				"view", viewName,
				"error", refreshStatus,
				"duration", duration)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating refresh results: %w", err)
	}

	return nil
}

// CleanupRealtimeDataBySourceAndType removes specific realtime data type for a source
// This granular approach ensures that if one cleanup fails, others can still succeed
func (m *Maintenance) CleanupRealtimeDataBySourceAndType(ctx context.Context, sourceID int, dataType RealtimeDataType) CleanupResult {
	result := CleanupResult{
		DataType: dataType,
		SourceID: sourceID,
		Success:  false,
	}

	m.logger.Info("Cleaning up realtime data by source and type", 
		"source_id", sourceID, 
		"data_type", dataType)

	var recordsDeleted int64

	switch dataType {
	case VehiclePositions:
		// Count first to track deletion
		err := m.db.DB().QueryRowContext(ctx, `
			SELECT COUNT(*) FROM gtfs_rt.vehicle_positions 
			WHERE feed_message_id IN (
				SELECT feed_message_id FROM gtfs_rt.feed_messages WHERE source_id = $1
			)`, sourceID).Scan(&recordsDeleted)
		if err != nil {
			result.Error = fmt.Sprintf("counting vehicle positions: %v", err)
			return result
		}

		// Delete vehicle positions
		_, err = m.db.DB().ExecContext(ctx, `
			DELETE FROM gtfs_rt.vehicle_positions 
			WHERE feed_message_id IN (
				SELECT feed_message_id FROM gtfs_rt.feed_messages WHERE source_id = $1
			)`, sourceID)
		if err != nil {
			result.Error = fmt.Sprintf("deleting vehicle positions: %v", err)
			return result
		}

	case StopTimeUpdates:
		// Count first
		err := m.db.DB().QueryRowContext(ctx, `
			SELECT COUNT(*) FROM gtfs_rt.stop_time_updates stu
			JOIN gtfs_rt.trip_updates tu ON stu.trip_update_id = tu.trip_update_id
			JOIN gtfs_rt.feed_messages fm ON tu.feed_message_id = fm.feed_message_id
			WHERE fm.source_id = $1`, sourceID).Scan(&recordsDeleted)
		if err != nil {
			result.Error = fmt.Sprintf("counting stop time updates: %v", err)
			return result
		}

		// Delete stop time updates
		_, err = m.db.DB().ExecContext(ctx, `
			DELETE FROM gtfs_rt.stop_time_updates 
			WHERE trip_update_id IN (
				SELECT tu.trip_update_id FROM gtfs_rt.trip_updates tu
				JOIN gtfs_rt.feed_messages fm ON tu.feed_message_id = fm.feed_message_id
				WHERE fm.source_id = $1
			)`, sourceID)
		if err != nil {
			result.Error = fmt.Sprintf("deleting stop time updates: %v", err)
			return result
		}

	case TripUpdates:
		// Count first
		err := m.db.DB().QueryRowContext(ctx, `
			SELECT COUNT(*) FROM gtfs_rt.trip_updates tu
			JOIN gtfs_rt.feed_messages fm ON tu.feed_message_id = fm.feed_message_id
			WHERE fm.source_id = $1`, sourceID).Scan(&recordsDeleted)
		if err != nil {
			result.Error = fmt.Sprintf("counting trip updates: %v", err)
			return result
		}

		// Delete trip updates
		_, err = m.db.DB().ExecContext(ctx, `
			DELETE FROM gtfs_rt.trip_updates 
			WHERE feed_message_id IN (
				SELECT feed_message_id FROM gtfs_rt.feed_messages WHERE source_id = $1
			)`, sourceID)
		if err != nil {
			result.Error = fmt.Sprintf("deleting trip updates: %v", err)
			return result
		}

	case Alerts:
		// Count first
		err := m.db.DB().QueryRowContext(ctx, `
			SELECT COUNT(*) FROM gtfs_rt.alerts a
			JOIN gtfs_rt.feed_messages fm ON a.feed_message_id = fm.feed_message_id
			WHERE fm.source_id = $1`, sourceID).Scan(&recordsDeleted)
		if err != nil {
			result.Error = fmt.Sprintf("counting alerts: %v", err)
			return result
		}

		// Delete alerts
		_, err = m.db.DB().ExecContext(ctx, `
			DELETE FROM gtfs_rt.alerts 
			WHERE feed_message_id IN (
				SELECT feed_message_id FROM gtfs_rt.feed_messages WHERE source_id = $1
			)`, sourceID)
		if err != nil {
			result.Error = fmt.Sprintf("deleting alerts: %v", err)
			return result
		}

	case FeedMessages:
		// Count first
		err := m.db.DB().QueryRowContext(ctx, `
			SELECT COUNT(*) FROM gtfs_rt.feed_messages WHERE source_id = $1`, sourceID).Scan(&recordsDeleted)
		if err != nil {
			result.Error = fmt.Sprintf("counting feed messages: %v", err)
			return result
		}

		// Delete feed messages (should be last)
		_, err = m.db.DB().ExecContext(ctx, `
			DELETE FROM gtfs_rt.feed_messages WHERE source_id = $1`, sourceID)
		if err != nil {
			result.Error = fmt.Sprintf("deleting feed messages: %v", err)
			return result
		}

	default:
		result.Error = fmt.Sprintf("unknown data type: %s", dataType)
		return result
	}

	result.RecordsDeleted = recordsDeleted
	result.Success = true

	m.logger.Info("Successfully cleaned up realtime data",
		"source_id", sourceID,
		"data_type", dataType,
		"records_deleted", recordsDeleted)

	return result
}

// CleanupAllRealtimeDataBySource removes all realtime data for a source with granular error handling
func (m *Maintenance) CleanupAllRealtimeDataBySource(ctx context.Context, sourceID int) []CleanupResult {
	m.logger.Info("Starting comprehensive realtime cleanup for source", "source_id", sourceID)

	// Define cleanup order - dependencies first
	cleanupOrder := []RealtimeDataType{
		VehiclePositions, // References feed_messages
		StopTimeUpdates,  // References trip_updates
		TripUpdates,      // Independent
		Alerts,           // Independent
		FeedMessages,     // Should be last (referenced by others)
	}

	var results []CleanupResult
	successCount := 0

	for _, dataType := range cleanupOrder {
		result := m.CleanupRealtimeDataBySourceAndType(ctx, sourceID, dataType)
		results = append(results, result)
		
		if result.Success {
			successCount++
		} else {
			m.logger.Error("Failed to cleanup realtime data type",
				"source_id", sourceID,
				"data_type", dataType,
				"error", result.Error)
		}
	}

	m.logger.Info("Completed realtime cleanup for source",
		"source_id", sourceID,
		"successful_cleanups", successCount,
		"total_cleanups", len(cleanupOrder))

	return results
}

// PerformPostImportMaintenance runs maintenance tasks after a successful GTFS import
func (m *Maintenance) PerformPostImportMaintenance(ctx context.Context) error {
	m.logger.Info("Performing post-import maintenance tasks")

	// Refresh materialized views
	if err := m.RefreshMaterializedViews(ctx); err != nil {
		return fmt.Errorf("refreshing materialized views: %w", err)
	}

	// Cleanup old versions (keep 1 inactive version as backup)
	_, err := m.CleanupOldGTFSVersions(ctx, 1)
	if err != nil {
		return fmt.Errorf("cleaning up old GTFS versions: %w", err)
	}

	m.logger.Info("Post-import maintenance completed successfully")
	return nil
}

// VacuumGTFSTables runs VACUUM ANALYZE on GTFS tables (must be called outside transaction)
func (m *Maintenance) VacuumGTFSTables(ctx context.Context) error {
	m.logger.Info("Starting VACUUM ANALYZE of GTFS tables")

	query := `SELECT * FROM gtfs.vacuum_gtfs_tables()`
	rows, err := m.db.DB().QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("executing vacuum_gtfs_tables: %w", err)
	}
	defer rows.Close()

	successCount := 0
	totalTables := 0

	for rows.Next() {
		var result VacuumResult
		
		err := rows.Scan(&result.TableName, &result.Operation, &result.Duration, &result.Status)
		if err != nil {
			return fmt.Errorf("scanning vacuum result: %w", err)
		}

		totalTables++
		
		durationStr := "unknown"
		if result.Duration != nil {
			durationStr = *result.Duration
		}

		if result.Status == "SUCCESS" {
			successCount++
			m.logger.Info("Vacuumed GTFS table successfully",
				"table", result.TableName,
				"operation", result.Operation,
				"duration", durationStr)
		} else {
			m.logger.Error("Failed to vacuum GTFS table",
				"table", result.TableName,
				"operation", result.Operation,
				"duration", durationStr,
				"error", result.Status)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating vacuum results: %w", err)
	}

	m.logger.Info("GTFS VACUUM ANALYZE completed", 
		"successful_tables", successCount,
		"total_tables", totalTables)
	
	if successCount < totalTables {
		return fmt.Errorf("vacuum failed for %d out of %d GTFS tables", totalTables-successCount, totalTables)
	}
	
	return nil
}