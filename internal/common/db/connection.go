package db

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/ptvtracker-data/internal/common/logger"
)

type DB struct {
	conn   *sql.DB
	logger logger.Logger
}

func New(connStr string, logger logger.Logger) (*DB, error) {
	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	logger.Info("Database connection established")

	return &DB{
		conn:   conn,
		logger: logger,
	}, nil
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return db.conn.BeginTx(ctx, nil)
}

// Logger returns the logger instance
func (db *DB) Logger() logger.Logger {
	return db.logger
}
