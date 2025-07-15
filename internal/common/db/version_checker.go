package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"
	
	"github.com/ptvtracker-data/pkg/gtfs/models"
)

type VersionChecker struct {
	db *DB
}

func NewVersionChecker(db *DB) *VersionChecker {
	return &VersionChecker{db: db}
}

func (vc *VersionChecker) GetActiveVersion(ctx context.Context) (*models.VersionInfo, error) {
	query := `
		SELECT version_id, version_name, created_at, updated_at, is_active, source_url, description
		FROM gtfs.versions
		WHERE is_active = true
		LIMIT 1
	`
	
	var version models.VersionInfo
	err := vc.db.conn.QueryRowContext(ctx, query).Scan(
		&version.VersionID,
		&version.VersionName,
		&version.CreatedAt,
		&version.UpdatedAt,
		&version.IsActive,
		&version.SourceURL,
		&version.Description,
	)
	
	if err == sql.ErrNoRows {
		vc.db.logger.Info("No active version found in database")
		return nil, nil
	}
	
	if err != nil {
		return nil, fmt.Errorf("querying active version: %w", err)
	}
	
	vc.db.logger.Debug("Found active version", 
		"version_id", version.VersionID,
		"version_name", version.VersionName,
		"updated_at", version.UpdatedAt)
	
	return &version, nil
}

func (vc *VersionChecker) HasNewerVersion(ctx context.Context, lastModified time.Time) (bool, error) {
	activeVersion, err := vc.GetActiveVersion(ctx)
	if err != nil {
		return false, fmt.Errorf("getting active version: %w", err)
	}
	
	// If no active version, we need to import
	if activeVersion == nil {
		vc.db.logger.Info("No active version found, new import needed")
		return true, nil
	}
	
	// Compare timestamps
	isNewer := lastModified.After(activeVersion.UpdatedAt)
	
	vc.db.logger.Info("Version comparison",
		"dataset_modified", lastModified,
		"active_version_updated", activeVersion.UpdatedAt,
		"is_newer", isNewer)
	
	return isNewer, nil
}

func (vc *VersionChecker) CreateNewVersion(ctx context.Context, versionName string, sourceURL string, lastModified time.Time) (int, error) {
	tx, err := vc.db.BeginTx(ctx)
	if err != nil {
		return 0, fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()
	
	// First, deactivate all existing versions
	_, err = tx.ExecContext(ctx, "UPDATE gtfs.versions SET is_active = false WHERE is_active = true")
	if err != nil {
		return 0, fmt.Errorf("deactivating versions: %w", err)
	}
	
	// Create new version
	var versionID int
	query := `
		INSERT INTO gtfs.versions (version_name, source_url, updated_at, is_active, description)
		VALUES ($1, $2, $3, false, $4)
		RETURNING version_id
	`
	
	description := fmt.Sprintf("GTFS data imported from %s at %s", sourceURL, lastModified.Format(time.RFC3339))
	err = tx.QueryRowContext(ctx, query, versionName, sourceURL, lastModified, description).Scan(&versionID)
	if err != nil {
		return 0, fmt.Errorf("creating version: %w", err)
	}
	
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("committing transaction: %w", err)
	}
	
	vc.db.logger.Info("Created new version",
		"version_id", versionID,
		"version_name", versionName)
	
	return versionID, nil
}

func (vc *VersionChecker) ActivateVersion(ctx context.Context, versionID int) error {
	tx, err := vc.db.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()
	
	// Deactivate all versions
	_, err = tx.ExecContext(ctx, "UPDATE gtfs.versions SET is_active = false WHERE is_active = true")
	if err != nil {
		return fmt.Errorf("deactivating versions: %w", err)
	}
	
	// Activate the specified version
	result, err := tx.ExecContext(ctx, "UPDATE gtfs.versions SET is_active = true WHERE version_id = $1", versionID)
	if err != nil {
		return fmt.Errorf("activating version: %w", err)
	}
	
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("getting rows affected: %w", err)
	}
	
	if rows == 0 {
		return fmt.Errorf("version %d not found", versionID)
	}
	
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	
	vc.db.logger.Info("Activated version", "version_id", versionID)
	return nil
}