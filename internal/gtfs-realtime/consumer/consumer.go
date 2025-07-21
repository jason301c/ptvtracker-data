package consumer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/ptvtracker-data/internal/common/config"
	"github.com/ptvtracker-data/internal/common/logger"
	gtfs_proto "github.com/ptvtracker-data/pkg/gtfs-realtime/proto"
	"google.golang.org/protobuf/proto"
)

const (
	HeaderAPIKey = "Ocp-Apim-Subscription-Key"
	UserAgent    = "ptvtracker-data/1.0"
)

type Consumer struct {
	config       config.GTFSRealtimeConfig
	httpClient   *http.Client
	logger       logger.Logger
	rateLimiter  *rateLimiter
	cache        *feedCache
	mu           sync.RWMutex
	isRunning    bool
	stopChan     chan struct{}
	feedChan     chan *FeedResult
}

type FeedResult struct {
	Endpoint  config.EndpointConfig
	Message   *gtfs_proto.FeedMessage
	Timestamp time.Time
	Error     error
}

type rateLimiter struct {
	tokens    chan struct{}
	interval  time.Duration
	lastReset time.Time
	mu        sync.Mutex
}

type feedCache struct {
	data map[string]*cacheEntry
	mu   sync.RWMutex
}

type cacheEntry struct {
	feedMessage *gtfs_proto.FeedMessage
	timestamp   time.Time
	etag        string
}

func NewConsumer(cfg config.GTFSRealtimeConfig, log logger.Logger) *Consumer {
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 5,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	return &Consumer{
		config:      cfg,
		httpClient:  client,
		logger:      log,
		rateLimiter: newRateLimiter(cfg.RateLimitPerMin),
		cache:       newFeedCache(),
		feedChan:    make(chan *FeedResult, 1000), // Increased buffer to handle high volume of Melbourne PT data
		stopChan:    make(chan struct{}),
	}
}

func newRateLimiter(requestsPerMin int) *rateLimiter {
	tokens := make(chan struct{}, requestsPerMin)
	for i := 0; i < requestsPerMin; i++ {
		tokens <- struct{}{}
	}

	return &rateLimiter{
		tokens:    tokens,
		interval:  time.Minute,
		lastReset: time.Now(),
	}
}

func newFeedCache() *feedCache {
	return &feedCache{
		data: make(map[string]*cacheEntry),
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isRunning {
		return fmt.Errorf("consumer is already running")
	}

	c.isRunning = true
	c.logger.Info("Starting GTFS-realtime consumer", "polling_interval", c.config.PollingInterval)

	// Start the rate limiter refresh goroutine
	go c.refreshRateLimiter(ctx)

	// Start polling goroutines for each endpoint
	for _, endpoint := range c.config.Endpoints {
		go c.pollEndpoint(ctx, endpoint)
	}

	return nil
}

func (c *Consumer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.isRunning {
		return
	}

	c.logger.Info("Stopping GTFS-realtime consumer")
	c.isRunning = false
	close(c.stopChan)
}

func (c *Consumer) FeedChannel() <-chan *FeedResult {
	return c.feedChan
}

func (c *Consumer) refreshRateLimiter(ctx context.Context) {
	ticker := time.NewTicker(c.rateLimiter.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		case <-ticker.C:
			c.rateLimiter.mu.Lock()
			// Refill rate limiter tokens
			for len(c.rateLimiter.tokens) < cap(c.rateLimiter.tokens) {
				select {
				case c.rateLimiter.tokens <- struct{}{}:
				default:
					break
				}
			}
			c.rateLimiter.lastReset = time.Now()
			c.rateLimiter.mu.Unlock()
		}
	}
}

func (c *Consumer) pollEndpoint(ctx context.Context, endpoint config.EndpointConfig) {
	ticker := time.NewTicker(c.config.PollingInterval)
	defer ticker.Stop()

	c.logger.Info("Starting endpoint polling", "endpoint", endpoint.Name, "url", endpoint.URL)

	// Initial fetch
	c.fetchFeed(ctx, endpoint)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		case <-ticker.C:
			c.fetchFeed(ctx, endpoint)
		}
	}
}

func (c *Consumer) fetchFeed(ctx context.Context, endpoint config.EndpointConfig) {
	result := &FeedResult{
		Endpoint:  endpoint,
		Timestamp: time.Now(),
	}

	defer func() {
		select {
		case c.feedChan <- result:
		case <-ctx.Done():
		case <-c.stopChan:
		default:
			c.logger.Warn("Feed channel is full, dropping result", "endpoint", endpoint.Name)
		}
	}()

	// Check rate limit
	select {
	case <-c.rateLimiter.tokens:
	case <-ctx.Done():
		result.Error = ctx.Err()
		return
	case <-c.stopChan:
		result.Error = fmt.Errorf("consumer stopped")
		return
	case <-time.After(5 * time.Second):
		result.Error = fmt.Errorf("rate limit exceeded")
		return
	}

	// Check cache
	if cachedEntry := c.cache.get(endpoint.Name); cachedEntry != nil {
		if time.Since(cachedEntry.timestamp) < c.config.CacheExpiration {
			result.Message = cachedEntry.feedMessage
			c.logger.Debug("Using cached feed", "endpoint", endpoint.Name)
			return
		}
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint.URL, nil)
	if err != nil {
		result.Error = fmt.Errorf("failed to create request: %w", err)
		return
	}

	req.Header.Set(HeaderAPIKey, c.config.APIKey)
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("Accept", "application/x-protobuf")

	// Add conditional headers if we have cached data
	if cachedEntry := c.cache.get(endpoint.Name); cachedEntry != nil && cachedEntry.etag != "" {
		req.Header.Set("If-None-Match", cachedEntry.etag)
	}

	// Make HTTP request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		result.Error = fmt.Errorf("failed to fetch feed: %w", err)
		return
	}
	defer resp.Body.Close()

	// Handle 304 Not Modified
	if resp.StatusCode == http.StatusNotModified {
		if cachedEntry := c.cache.get(endpoint.Name); cachedEntry != nil {
			result.Message = cachedEntry.feedMessage
			c.logger.Debug("Feed not modified, using cached version", "endpoint", endpoint.Name)
			return
		}
	}

	// Check response status
	if resp.StatusCode != http.StatusOK {
		result.Error = fmt.Errorf("HTTP error: %d %s", resp.StatusCode, resp.Status)
		return
	}

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		result.Error = fmt.Errorf("failed to read response body: %w", err)
		return
	}

	// Parse protobuf
	feedMessage := &gtfs_proto.FeedMessage{}
	if err := proto.Unmarshal(body, feedMessage); err != nil {
		result.Error = fmt.Errorf("failed to unmarshal protobuf: %w", err)
		return
	}

	// Cache the result
	etag := resp.Header.Get("ETag")
	c.cache.set(endpoint.Name, &cacheEntry{
		feedMessage: feedMessage,
		timestamp:   time.Now(),
		etag:        etag,
	})

	result.Message = feedMessage
	c.logger.Debug("Successfully fetched feed", "endpoint", endpoint.Name, "entities", len(feedMessage.Entity))
}

func (fc *feedCache) get(key string) *cacheEntry {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return fc.data[key]
}

func (fc *feedCache) set(key string, entry *cacheEntry) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.data[key] = entry
}