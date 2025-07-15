package scraper

import (
	"context"
	"time"

	"github.com/ptvtracker-data/pkg/gtfs-static/models"
)

type MetadataFetcher interface {
	FetchMetadata(ctx context.Context, url string) (*models.DatasetMetadata, error)
}

type VersionChecker interface {
	GetActiveVersion(ctx context.Context) (*models.VersionInfo, error)
	HasNewerVersion(ctx context.Context, lastModified time.Time) (bool, error)
}

type Downloader interface {
	Download(ctx context.Context, url string, destPath string) error
}

type Importer interface {
	Import(ctx context.Context, filePath string, versionName string) error
}

type Scheduler interface {
	Start(ctx context.Context) error
	Stop() error
}
