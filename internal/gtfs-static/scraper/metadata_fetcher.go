package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
	
	"github.com/ptvtracker-data/pkg/gtfs/models"
	"github.com/ptvtracker-data/internal/common/logger"
)

const (
	dataVicAPIBase = "https://discover.data.vic.gov.au/api/3/action/resource_show"
	httpTimeout    = 30 * time.Second
)

type HTTPMetadataFetcher struct {
	client *http.Client
	logger logger.Logger
}

func NewHTTPMetadataFetcher(logger logger.Logger) *HTTPMetadataFetcher {
	return &HTTPMetadataFetcher{
		client: &http.Client{
			Timeout: httpTimeout,
		},
		logger: logger,
	}
}

func (f *HTTPMetadataFetcher) FetchMetadata(ctx context.Context, resourceID string) (*models.DatasetMetadata, error) {
	url := fmt.Sprintf("%s?id=%s", dataVicAPIBase, resourceID)
	
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	
	f.logger.Debug("Fetching metadata", "url", url, "resource_id", resourceID)
	
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	
	var result models.DatasetResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	
	if !result.Success {
		return nil, fmt.Errorf("API returned success=false")
	}
	
	f.logger.Info("Metadata fetched successfully", 
		"resource_id", resourceID,
		"last_modified", result.Result.LastModified,
		"format", result.Result.Format)
	
	return &result.Result, nil
}