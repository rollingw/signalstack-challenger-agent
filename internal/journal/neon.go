package journal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

type NeonJournal struct {
	db *sql.DB
}

// NewNeonJournal connects to Neon Cloud PostgreSQL and ensures execution_journal table exists.
func NewNeonJournal(connURL string) (*NeonJournal, error) {
	if connURL == "" {
		return nil, nil // Neon sync disabled
	}

	db, err := sql.Open("postgres", connURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open neon postgres connection: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(10 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping neon postgres: %w", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS execution_journal (
		id SERIAL PRIMARY KEY,
		signal_id VARCHAR(128) UNIQUE NOT NULL,
		broker VARCHAR(64) NOT NULL,
		order_id VARCHAR(128),
		client_order_id VARCHAR(128),
		symbol VARCHAR(32) NOT NULL,
		action VARCHAR(16) NOT NULL,
		requested_shares NUMERIC NOT NULL,
		filled_shares NUMERIC DEFAULT 0,
		signal_price NUMERIC NOT NULL,
		fill_price NUMERIC DEFAULT 0,
		status VARCHAR(64) NOT NULL,
		strategy VARCHAR(64),
		submitted_at TIMESTAMPTZ,
		filled_at TIMESTAMPTZ,
		error_message TEXT,
		raw_response TEXT,
		created_at TIMESTAMPTZ DEFAULT NOW(),
		updated_at TIMESTAMPTZ DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_neon_exec_signal_id ON execution_journal(signal_id);
	`
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize neon execution_journal table: %w", err)
	}

	return &NeonJournal{db: db}, nil
}

// Close closes the Neon DB connection.
func (n *NeonJournal) Close() error {
	if n.db != nil {
		return n.db.Close()
	}
	return nil
}

// RecordExecution inserts a journal entry into Neon.
func (n *NeonJournal) RecordExecution(ctx context.Context, rec *ExecutionRecord) error {
	if n == nil || n.db == nil {
		return nil
	}

	query := `
	INSERT INTO execution_journal (
		signal_id, broker, order_id, client_order_id, symbol, action,
		requested_shares, filled_shares, signal_price, fill_price,
		status, strategy, submitted_at, filled_at, error_message, raw_response, created_at, updated_at
	) VALUES (
		$1, $2, $3, $4, $5, $6,
		$7, $8, $9, $10,
		$11, $12, $13, $14, $15, $16, $17, NOW()
	)
	ON CONFLICT (signal_id) DO UPDATE SET
		order_id = EXCLUDED.order_id,
		filled_shares = EXCLUDED.filled_shares,
		fill_price = EXCLUDED.fill_price,
		status = EXCLUDED.status,
		filled_at = EXCLUDED.filled_at,
		error_message = EXCLUDED.error_message,
		raw_response = EXCLUDED.raw_response,
		updated_at = NOW();
	`
	_, err := n.db.ExecContext(ctx, query,
		rec.SignalID, rec.Broker, rec.OrderID, rec.ClientOrderID, rec.Symbol, rec.Action,
		rec.RequestedShares, rec.FilledShares, rec.SignalPrice, rec.FillPrice,
		rec.Status, rec.Strategy, rec.SubmittedAt, rec.FilledAt, rec.ErrorMessage, rec.RawResponse, rec.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert record in neon: %w", err)
	}
	return nil
}

// UpdateFill updates fill details for a record in Neon.
func (n *NeonJournal) UpdateFill(ctx context.Context, rec *ExecutionRecord) error {
	if n == nil || n.db == nil {
		return nil
	}

	query := `
	UPDATE execution_journal
	SET order_id = $1, filled_shares = $2, fill_price = $3, status = $4,
	    filled_at = $5, error_message = $6, raw_response = $7, updated_at = NOW()
	WHERE signal_id = $8;
	`
	_, err := n.db.ExecContext(ctx, query,
		rec.OrderID, rec.FilledShares, rec.FillPrice, rec.Status,
		rec.FilledAt, rec.ErrorMessage, rec.RawResponse, rec.SignalID,
	)
	if err != nil {
		return fmt.Errorf("failed to update fill in neon: %w", err)
	}
	return nil
}
