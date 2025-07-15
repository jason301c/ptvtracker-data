package logger

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/natefinch/lumberjack"
	"github.com/rs/zerolog"
)

// Logger is the global logger abstraction
var (
	logger     zerolog.Logger
	loggerOnce sync.Once
)

// LoggerConfig holds configuration for the logger
type LoggerConfig struct {
	Level           zerolog.Level
	Console         bool
	File            bool
	FilePath        string
	MaxSizeMB       int
	MaxBackups      int
	MaxAgeDays      int
	Compress        bool
	TimeFieldFormat string
}

// InitLogger initializes the global logger with the given config
func InitLogger(cfg LoggerConfig) {
	loggerOnce.Do(func() {
		var writers []io.Writer

		if cfg.Console {
			consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: cfg.TimeFieldFormat}
			writers = append(writers, consoleWriter)
		}

		if cfg.File {
			fileWriter := &lumberjack.Logger{
				Filename:   cfg.FilePath,
				MaxSize:    cfg.MaxSizeMB,
				MaxBackups: cfg.MaxBackups,
				MaxAge:     cfg.MaxAgeDays,
				Compress:   cfg.Compress,
			}
			writers = append(writers, fileWriter)
		}

		multi := io.MultiWriter(writers...)
		logger = zerolog.New(multi).With().Timestamp().Logger().Level(cfg.Level)
		zerolog.TimeFieldFormat = cfg.TimeFieldFormat
	})
}

// Info logs an info message
func Info(msg string, fields ...interface{}) {
	logWithFields(logger.Info(), msg, fields...)
}

// Warn logs a warning message
func Warn(msg string, fields ...interface{}) {
	logWithFields(logger.Warn(), msg, fields...)
}

// Error logs an error message
func Error(msg string, fields ...interface{}) {
	logWithFields(logger.Error(), msg, fields...)
}

// Debug logs a debug message
func Debug(msg string, fields ...interface{}) {
	logWithFields(logger.Debug(), msg, fields...)
}

// Fatal logs a fatal message and exits
func Fatal(msg string, fields ...interface{}) {
	logWithFields(logger.Fatal(), msg, fields...)
}

// logWithFields adds structured fields to the event
func logWithFields(event *zerolog.Event, msg string, fields ...interface{}) {
	if len(fields) == 1 {
		if m, ok := fields[0].(map[string]interface{}); ok {
			event.Fields(m).Msg(msg)
			return
		}
	}
	// fallback: treat as key-value pairs
	if len(fields)%2 == 0 {
		for i := 0; i < len(fields); i += 2 {
			key, ok := fields[i].(string)
			if !ok {
				continue
			}
			event = event.Interface(key, fields[i+1])
		}
	}
	event.Msg(msg)
}

// GetLogger returns the underlying zerolog.Logger (for advanced use)
func GetLogger() zerolog.Logger {
	return logger
}

// Example default config
func DefaultLoggerConfig() LoggerConfig {
	return LoggerConfig{
		Level:           zerolog.InfoLevel,
		Console:         true,
		File:            true,
		FilePath:        "app.log",
		MaxSizeMB:       10,
		MaxBackups:      5,
		MaxAgeDays:      30,
		Compress:        true,
		TimeFieldFormat: time.RFC3339,
	}
}
