package discord

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type WebhookMessage struct {
	Content string  `json:"content"`
	Embeds  []Embed `json:"embeds,omitempty"`
}

type Embed struct {
	Title       string    `json:"title"`
	Description string    `json:"description"`
	Color       int       `json:"color"`
	Timestamp   time.Time `json:"timestamp"`
	Fields      []Field   `json:"fields,omitempty"`
}

type Field struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

type Client struct {
	webhookURL string
	httpClient *http.Client
}

func NewClient(webhookURL string) *Client {
	return &Client{
		webhookURL: webhookURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) SendMessage(msg WebhookMessage) error {
	if c.webhookURL == "" {
		return nil
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook message: %w", err)
	}

	req, err := http.NewRequest("POST", c.webhookURL, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send webhook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook request failed with status: %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) SendLogMessage(level, message string, fields map[string]interface{}) error {
	color := getColorForLevel(level)
	
	embed := Embed{
		Title:       fmt.Sprintf("🚨 %s Log Alert", level),
		Description: message,
		Color:       color,
		Timestamp:   time.Now(),
	}

	if len(fields) > 0 {
		for key, value := range fields {
			embed.Fields = append(embed.Fields, Field{
				Name:   key,
				Value:  fmt.Sprintf("%v", value),
				Inline: true,
			})
		}
	}

	msg := WebhookMessage{
		Embeds: []Embed{embed},
	}

	return c.SendMessage(msg)
}

func getColorForLevel(level string) int {
	switch level {
	case "ERROR":
		return 0xFF0000 // Red
	case "FATAL":
		return 0x8B0000 // Dark Red
	case "WARN":
		return 0xFFA500 // Orange
	default:
		return 0x808080 // Gray
	}
}