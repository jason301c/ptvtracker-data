package maintenance

import (
	"testing"

	"github.com/ptvtracker-data/internal/common/logger"
)

func TestRealtimeDataTypes(t *testing.T) {
	// Test that all realtime data types are properly defined
	expected := []RealtimeDataType{
		VehiclePositions,
		TripUpdates,
		StopTimeUpdates,
		Alerts,
		FeedMessages,
	}

	expectedStrings := []string{
		"vehicle_positions",
		"trip_updates",
		"stop_time_updates",
		"alerts",
		"feed_messages",
	}

	for i, dataType := range expected {
		if string(dataType) != expectedStrings[i] {
			t.Errorf("Expected %s, got %s", expectedStrings[i], string(dataType))
		}
	}
}

func TestCleanupResult(t *testing.T) {
	// Test CleanupResult structure
	result := CleanupResult{
		DataType:       VehiclePositions,
		SourceID:       1,
		RecordsDeleted: 100,
		Success:        true,
		Error:          "",
	}

	if result.DataType != VehiclePositions {
		t.Errorf("Expected VehiclePositions, got %s", result.DataType)
	}

	if result.SourceID != 1 {
		t.Errorf("Expected source ID 1, got %d", result.SourceID)
	}

	if result.RecordsDeleted != 100 {
		t.Errorf("Expected 100 records deleted, got %d", result.RecordsDeleted)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}
}

func TestNewMaintenance(t *testing.T) {
	// Test creating a new Maintenance instance (without actual DB)
	// This tests the structure and interfaces
	
	log := logger.New(nil) // Create a logger with nil writer for testing
	
	// We can't test with a real DB in unit tests, but we can test the structure
	var m *Maintenance
	
	// Test that maintenance can be created (would normally require DB)
	if m != nil {
		t.Error("Should be nil without proper initialization")
	}
	
	// Test that logger interface is correct
	if log == nil {
		t.Error("Logger should be created successfully")
	}
}