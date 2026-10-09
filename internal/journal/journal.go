package journal

import (
	"context"
	"log"
	"time"
)

// ExecutionRecord represents an entry in the execution journal linking the
// Python trade signal to the broker order ID and execution status.
type ExecutionRecord struct {
	ID              int64      `json:"id,omitempty"`
	SignalID        string     `json:"signal_id"`
	Broker          string     `json:"broker"`
	OrderID         string     `json:"order_id"`
	ClientOrderID   string     `json:"client_order_id"`
	Symbol          string     `json:"symbol"`
	Action          string     `json:"action"`
	RequestedShares float64    `json:"requested_shares"`
	FilledShares    float64    `json:"filled_shares"`
	SignalPrice     float64    `json:"signal_price"`
	FillPrice       float64    `json:"fill_price"`
	Status          string     `json:"status"` // "filled", "accepted", "rejected", "failed", "skipped_bootstrap_unheld"
	Strategy        string     `json:"strategy"`
	SubmittedAt     time.Time  `json:"submitted_at"`
	FilledAt        *time.Time `json:"filled_at,omitempty"`
	ErrorMessage    string     `json:"error_message,omitempty"`
	RawResponse     string     `json:"raw_response,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
}

// PaperTrade represents a realized closed trade or execution fill.
type PaperTrade struct {
	ID         int64     `json:"id"`
	Symbol     string    `json:"symbol"`
	Action     string    `json:"action"`
	Shares     float64   `json:"shares"`
	Price      float64   `json:"price"`
	RealizedPL float64   `json:"realized_pl"`
	SignalID   string    `json:"signal_id"`
	Timestamp  time.Time `json:"timestamp"`
}

// MultiJournal coordinates writing execution records to SQLite (for local idempotency and state),
// PocketBase (for app collection sync), and Neon (PostgreSQL cloud database).
type MultiJournal struct {
	sqlite     *SQLiteJournal
	pocketbase *PocketBaseJournal
	neon       *NeonJournal
}

// NewMultiJournal creates a coordinated MultiJournal.
func NewMultiJournal(sqlite *SQLiteJournal, pb *PocketBaseJournal, neon *NeonJournal) *MultiJournal {
	return &MultiJournal{
		sqlite:     sqlite,
		pocketbase: pb,
		neon:       neon,
	}
}

func (m *MultiJournal) SQLite() *SQLiteJournal {
	return m.sqlite
}

// IsDuplicate checks whether the given signal_id has already been processed.
func (m *MultiJournal) IsDuplicate(ctx context.Context, signalID string) (bool, *ExecutionRecord, error) {
	if m.sqlite == nil {
		return false, nil, nil
	}
	return m.sqlite.IsDuplicate(ctx, signalID)
}

// RecordExecution stores an execution record across SQLite, PocketBase, and Neon.
func (m *MultiJournal) RecordExecution(ctx context.Context, rec *ExecutionRecord) error {
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}

	// 1. SQLite (authoritative local store)
	if m.sqlite != nil {
		if err := m.sqlite.RecordExecution(ctx, rec); err != nil {
			return err
		}
	}

	// 2. PocketBase (best-effort sync)
	if m.pocketbase != nil {
		go func(r ExecutionRecord) {
			if err := m.pocketbase.RecordExecution(context.Background(), &r); err != nil {
				log.Printf("[MultiJournal] PocketBase sync warning: %v", err)
			}
		}(*rec)
	}

	// 3. Neon (best-effort sync)
	if m.neon != nil {
		go func(r ExecutionRecord) {
			if err := m.neon.RecordExecution(context.Background(), &r); err != nil {
				log.Printf("[MultiJournal] Neon sync warning: %v", err)
			}
		}(*rec)
	}

	return nil
}

// UpdateFill updates the final fill details across stores.
func (m *MultiJournal) UpdateFill(ctx context.Context, rec *ExecutionRecord) error {
	if m.sqlite != nil {
		if err := m.sqlite.UpdateFill(ctx, rec); err != nil {
			return err
		}
	}

	if m.pocketbase != nil {
		go func(r ExecutionRecord) {
			if err := m.pocketbase.UpdateFill(context.Background(), &r); err != nil {
				log.Printf("[MultiJournal] PocketBase fill sync warning: %v", err)
			}
		}(*rec)
	}

	if m.neon != nil {
		go func(r ExecutionRecord) {
			if err := m.neon.UpdateFill(context.Background(), &r); err != nil {
				log.Printf("[MultiJournal] Neon fill sync warning: %v", err)
			}
		}(*rec)
	}

	return nil
}

// GetRecentExecutions retrieves recent journal entries from SQLite.
func (m *MultiJournal) GetRecentExecutions(ctx context.Context, limit int) ([]*ExecutionRecord, error) {
	if m.sqlite != nil {
		return m.sqlite.GetRecentExecutions(ctx, limit)
	}
	return []*ExecutionRecord{}, nil
}

// Close closes any active database connections.
func (m *MultiJournal) Close() {
	if m.sqlite != nil {
		m.sqlite.Close()
	}
	if m.neon != nil {
		m.neon.Close()
	}
}
