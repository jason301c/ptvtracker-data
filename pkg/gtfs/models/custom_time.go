package models

import (
	"fmt"
	"strings"
	"time"
)

// CustomTime handles time parsing for Victorian data portal timestamps
// which don't include timezone information
type CustomTime struct {
	time.Time
}

// UnmarshalJSON handles parsing of timestamps without timezone
func (ct *CustomTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	if s == "null" || s == "" {
		return nil
	}
	
	// Try parsing with different formats
	formats := []string{
		"2006-01-02T15:04:05.999999", // Format from API
		"2006-01-02T15:04:05",         // Without microseconds
		time.RFC3339,                  // Standard format with timezone
		time.RFC3339Nano,              // With nanoseconds
	}
	
	var parseErr error
	for _, format := range formats {
		t, err := time.Parse(format, s)
		if err == nil {
			// Assume Melbourne timezone (Australia/Melbourne) for timestamps without timezone
			if !strings.Contains(s, "Z") && !strings.Contains(s, "+") && !strings.Contains(s, "-") {
				loc, err := time.LoadLocation("Australia/Melbourne")
				if err == nil {
					t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc)
				}
			}
			ct.Time = t
			return nil
		}
		parseErr = err
	}
	
	return fmt.Errorf("unable to parse time %q: %w", s, parseErr)
}

// MarshalJSON converts the time back to JSON
func (ct CustomTime) MarshalJSON() ([]byte, error) {
	if ct.Time.IsZero() {
		return []byte("null"), nil
	}
	return []byte(fmt.Sprintf("\"%s\"", ct.Time.Format(time.RFC3339))), nil
}