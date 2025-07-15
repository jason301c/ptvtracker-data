package scraper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ptvtracker-data/internal/common/logger"
	"github.com/ptvtracker-data/pkg/gtfs-static/models"
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

func (f *HTTPMetadataFetcher) FetchMetadata(ctx context.Context, url string) (*models.DatasetMetadata, error) {
	// Split URL into resource ID
	resourceID := strings.Split(url, "/")[len(strings.Split(url, "/"))-1]

	url = fmt.Sprintf("%s?id=%s", dataVicAPIBase, resourceID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	f.logger.Debug("Fetching metadata", "url", url, "resource_id", resourceID)

	resp, err := f.client.Do(req)
	if err != nil {
		f.logger.Error("Failed to execute request", "url", url, "error", err)
		return nil, fmt.Errorf("executing request to %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Try to read the response body for error details
		body, _ := io.ReadAll(resp.Body)
		f.logger.Error("API returned error status",
			"status_code", resp.StatusCode,
			"url", url,
			"response_body", string(body))
		return nil, fmt.Errorf("API returned status %d: %s", resp.StatusCode, string(body))
	}

	var result models.DatasetResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	if !result.Success {
		f.logger.Error("API returned success=false",
			"url", url,
			"result", result)
		return nil, fmt.Errorf("API returned success=false for resource %s", resourceID)
	}

	f.logger.Info("Metadata fetched successfully",
		"resource_id", resourceID,
		"last_modified", result.Result.LastModified.Time,
		"format", result.Result.Format)

	return &result.Result, nil
}
