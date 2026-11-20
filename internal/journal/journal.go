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

// PositionZeroLine anchors an open position's immutable entry pivot ("zero line")
// against its inverse Direxion partner for Zero-Line Flip & Trend-Follow tracking.
type PositionZeroLine struct {
	ID                 int64     `json:"id,omitempty"`
	PairKey            string    `json:"pair_key"`
	EntrySignalID      string    `json:"entry_signal_id"`
	ActiveSymbol       string    `json:"active_symbol"`
	PartnerSymbol      string    `json:"partner_symbol"`
	ArmedDate          string    `json:"armed_date"`
	PivotLabel         string    `json:"pivot_label"`
	ActiveZeroPrice    float64   `json:"active_zero_price"`
	PartnerZeroPrice   float64   `json:"partner_zero_price"`
	ActiveEntryPrice   float64   `json:"active_entry_price"`
	PartnerEntryPrice  float64   `json:"partner_entry_price"`
	ActiveLivePrice    float64   `json:"active_live_price"`
	PartnerLivePrice   float64   `json:"partner_live_price"`
	ActiveVsZeroPct    float64   `json:"active_vs_zero_pct"`
	PartnerVsZeroPct   float64   `json:"partner_vs_zero_pct"`
	ActiveLinearState  string    `json:"active_linear_state"`
	PartnerLinearState string    `json:"partner_linear_state"`
	FlipCount          int       `json:"flip_count"`
	ZeroLineStatus     string    `json:"zero_line_status"`
	State              string    `json:"state"`
	RepaintCount       int       `json:"repaint_count"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// PositionRepaint records an immutable audit event whenever an active position's
// entry pivot repaints, breaches the zero line, flips to its opposite partner, or stops out.
type PositionRepaint struct {
	ID                 int64     `json:"id,omitempty"`
	PairKey            string    `json:"pair_key"`
	Symbol             string    `json:"symbol"`
	PartnerSymbol      string    `json:"partner_symbol"`
	BarDate            string    `json:"bar_date"`
	EventType          string    `json:"event_type"`
	OrigPivotPrice     float64   `json:"orig_pivot_price"`
	NewRepaintPrice    float64   `json:"new_repaint_price"`
	ActiveLivePrice    float64   `json:"active_live_price"`
	PartnerLivePrice   float64   `json:"partner_live_price"`
	ActiveVsZeroPct    float64   `json:"active_vs_zero_pct"`
	PartnerVsZeroPct   float64   `json:"partner_vs_zero_pct"`
	PartnerLinearState string    `json:"partner_linear_state"`
	ActionTaken        string    `json:"action_taken"`
	Note               string    `json:"note"`
	DetectedAt         time.Time `json:"detected_at"`
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

func (m *MultiJournal) Neon() *NeonJournal {
	return m.neon
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
