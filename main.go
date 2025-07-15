package main

import (
	"fmt"
	"ptvtracker-data/utils/logger"
)

func main() {
	fmt.Println("--- Logging Abstraction Example ---")

	// Initialize the logger abstraction
	logger.InitLogger(logger.DefaultLoggerConfig())

	logger.Info("App started", map[string]interface{}{"module": "main", "version": "1.0.0"})
	logger.Debug("Debugging details", "user_id", 123, "active", true)
	logger.Warn("This is a warning", "threshold", 80)
	logger.Error("An error occurred", "error_code", 500, "reason", "Internal Server Error")
	// Uncomment for fatal example (will exit the app)
	// logger.Fatal("Fatal error, shutting down", "shutdown", true)
}
