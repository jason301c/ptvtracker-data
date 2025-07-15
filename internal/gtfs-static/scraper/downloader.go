package scraper

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"
	
	"github.com/ptvtracker-data/internal/common/logger"
)

type HTTPDownloader struct {
	client *http.Client
	logger logger.Logger
}

func NewHTTPDownloader(logger logger.Logger) *HTTPDownloader {
	return &HTTPDownloader{
		client: &http.Client{
			Timeout: 5 * time.Minute, // Large files may take time
		},
		logger: logger,
	}
}

func (d *HTTPDownloader) Download(ctx context.Context, url string, destPath string) error {
	// Ensure destination directory exists
	destDir := filepath.Dir(destPath)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("creating destination directory: %w", err)
	}
	
	// Create temporary file
	tempFile, err := os.CreateTemp(destDir, "gtfs_download_*.tmp")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath) // Clean up temp file on any error
	
	d.logger.Info("Starting download", "url", url, "dest", destPath)
	
	// Create request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		tempFile.Close()
		return fmt.Errorf("creating request: %w", err)
	}
	
	// Execute request
	resp, err := d.client.Do(req)
	if err != nil {
		tempFile.Close()
		return fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		tempFile.Close()
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	
	// Download with progress tracking
	written, err := d.copyWithProgress(tempFile, resp.Body, resp.ContentLength)
	tempFile.Close()
	
	if err != nil {
		return fmt.Errorf("downloading file: %w", err)
	}
	
	// Move temp file to final destination
	if err := os.Rename(tempPath, destPath); err != nil {
		return fmt.Errorf("moving file to destination: %w", err)
	}
	
	d.logger.Info("Download completed", 
		"url", url,
		"dest", destPath,
		"size_bytes", written)
	
	return nil
}

func (d *HTTPDownloader) copyWithProgress(dst io.Writer, src io.Reader, totalSize int64) (int64, error) {
	buf := make([]byte, 32*1024) // 32KB buffer
	var written int64
	lastLog := time.Now()
	
	for {
		nr, err := src.Read(buf)
		if nr > 0 {
			nw, err := dst.Write(buf[0:nr])
			if err != nil {
				return written, err
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
			written += int64(nw)
			
			// Log progress every 5 seconds
			if time.Since(lastLog) > 5*time.Second && totalSize > 0 {
				progress := float64(written) / float64(totalSize) * 100
				d.logger.Debug("Download progress",
					"progress_percent", fmt.Sprintf("%.1f", progress),
					"bytes_downloaded", written,
					"total_bytes", totalSize)
				lastLog = time.Now()
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return written, err
		}
	}
	
	return written, nil
}