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

	// Set search_path for the connection pool
	if _, err := conn.Exec("SET search_path TO gtfs_rt, gtfs, public"); err != nil {
		return nil, fmt.Errorf("setting search path: %w", err)
	}

	logger.Info("Database connection established with search_path")

	return &DB{
		conn:   conn,
		logger: logger,
	}, nil
}


func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) BeginTx(ctx context.Context) (*sql.Tx, error) {
	tx, err := db.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	
	// Set search_path for this transaction to ensure it has access to all schemas
	if _, err := tx.Exec("SET search_path TO gtfs_rt, gtfs, public"); err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("setting search path for transaction: %w", err)
	}
	
	return tx, nil
}

// Logger returns the logger instance
func (db *DB) Logger() logger.Logger {
	return db.logger
}

// DB returns the underlying sql.DB connection
func (db *DB) DB() *sql.DB {
	return db.conn
}
