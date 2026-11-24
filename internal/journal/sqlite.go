package journal

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/rollingw/signalstack-agent/internal/broker"
	_ "modernc.org/sqlite"
)

type SQLiteJournal struct {
	db            *sql.DB
	accountID     string
	accountNumber string
}

var cstLocation = func() *time.Location {
	loc, err := time.LoadLocation("America/Chicago")
	if err != nil {
		return time.FixedZone("CST", -6*3600)
	}
	return loc
}()

func getStartOfTodayCST() time.Time {
	nowCST := time.Now().In(cstLocation)
	return time.Date(nowCST.Year(), nowCST.Month(), nowCST.Day(), 0, 0, 0, 0, cstLocation)
}

// getPrevTradingCloseCST returns the market close timestamp (15:00 CST / 16:00 EST)
// of the most recent prior regular trading session.
// Weekend-opened signals/positions (e.g. Saturday or Sunday) execute on Monday morning,
// so their day movement for Monday must be measured from entry fill, NOT Friday close.
func getPrevTradingCloseCST() time.Time {
	nowCST := time.Now().In(cstLocation)
	weekday := nowCST.Weekday()
	daysBack := 1
	if weekday == time.Monday {
		daysBack = 3
	} else if weekday == time.Sunday {
		daysBack = 2
	} else if weekday == time.Saturday {
		daysBack = 1
	}
	prevDay := nowCST.AddDate(0, 0, -daysBack)
	return time.Date(prevDay.Year(), prevDay.Month(), prevDay.Day(), 15, 0, 0, 0, cstLocation)
}

// NewSQLiteJournal initializes the SQLite database and ensures all required tables exist.
func NewSQLiteJournal(dbPath string, initialCapital float64, marginMultiplier float64, bootstrapDate string) (*SQLiteJournal, error) {
	if dir := filepath.Dir(dbPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create sqlite directory %s: %w", dir, err)
		}
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database at %s: %w", dbPath, err)
	}

	// Enable WAL mode for high performance concurrent reads and writes
	if _, err := db.Exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set pragma on sqlite: %w", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS execution_journal (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		signal_id TEXT UNIQUE NOT NULL,
		broker TEXT NOT NULL,
		order_id TEXT,
		client_order_id TEXT,
		symbol TEXT NOT NULL,
		action TEXT NOT NULL,
		requested_shares REAL NOT NULL,
		filled_shares REAL DEFAULT 0,
		signal_price REAL NOT NULL,
		fill_price REAL DEFAULT 0,
		status TEXT NOT NULL,
		strategy TEXT,
		submitted_at DATETIME,
		filled_at DATETIME,
		error_message TEXT,
		raw_response TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_exec_signal_id ON execution_journal(signal_id);
	CREATE INDEX IF NOT EXISTS idx_exec_order_id ON execution_journal(order_id);

	CREATE TABLE IF NOT EXISTS paper_account (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		initial_cash REAL NOT NULL,
		current_cash REAL NOT NULL,
		realized_pl REAL DEFAULT 0,
		margin_multiplier REAL DEFAULT 2.0,
		bootstrap_started_at DATETIME NOT NULL,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS paper_positions (
		symbol TEXT PRIMARY KEY,
		shares REAL NOT NULL,
		avg_entry_price REAL NOT NULL,
		cost_basis REAL NOT NULL,
		current_price REAL NOT NULL,
		unrealized_pl REAL DEFAULT 0,
		opened_at DATETIME NOT NULL,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS paper_trades (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		symbol TEXT NOT NULL,
		action TEXT NOT NULL,
		shares REAL NOT NULL,
		price REAL NOT NULL,
		realized_pl REAL DEFAULT 0,
		signal_id TEXT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS position_zero_lines (
		active_symbol TEXT PRIMARY KEY,
		pair_key TEXT NOT NULL,
		entry_signal_id TEXT DEFAULT '',
		partner_symbol TEXT NOT NULL,
		armed_date TEXT DEFAULT '',
		pivot_label TEXT DEFAULT '',
		active_zero_price REAL DEFAULT 0,
		partner_zero_price REAL DEFAULT 0,
		active_entry_price REAL DEFAULT 0,
		partner_entry_price REAL DEFAULT 0,
		active_live_price REAL DEFAULT 0,
		partner_live_price REAL DEFAULT 0,
		active_vs_zero_pct REAL DEFAULT 0,
		partner_vs_zero_pct REAL DEFAULT 0,
		active_linear_state TEXT DEFAULT '',
		partner_linear_state TEXT DEFAULT '',
		flip_count INTEGER DEFAULT 0,
		zero_line_status TEXT DEFAULT 'ABOVE_ZERO',
		state TEXT DEFAULT 'OPEN',
		repaint_count INTEGER DEFAULT 0,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS position_repaints (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		pair_key TEXT NOT NULL,
		symbol TEXT NOT NULL,
		partner_symbol TEXT NOT NULL,
		bar_date TEXT NOT NULL,
		event_type TEXT NOT NULL,
		orig_pivot_price REAL DEFAULT 0,
		new_repaint_price REAL DEFAULT 0,
		active_live_price REAL DEFAULT 0,
		partner_live_price REAL DEFAULT 0,
		active_vs_zero_pct REAL DEFAULT 0,
		partner_vs_zero_pct REAL DEFAULT 0,
		partner_linear_state TEXT DEFAULT '',
		action_taken TEXT DEFAULT 'HOLD',
		note TEXT DEFAULT '',
		detected_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		UNIQUE(pair_key, symbol, bar_date, event_type)
	);
	CREATE INDEX IF NOT EXISTS idx_pos_repaints_detected ON position_repaints(detected_at DESC);
	`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize sqlite schema: %w", err)
	}

	// Migration: ensure prev_close column exists on paper_positions and last_equity on paper_account
	_, _ = db.Exec("ALTER TABLE paper_positions ADD COLUMN prev_close REAL DEFAULT 0;")
	_, _ = db.Exec("ALTER TABLE paper_account ADD COLUMN last_equity REAL DEFAULT 0;")

	// Initialize paper account if not already initialized
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM paper_account WHERE id = 1").Scan(&count)
	if count == 0 {
		if initialCapital <= 0 {
			initialCapital = 100000.0
		}
		if marginMultiplier <= 0 {
			marginMultiplier = 2.0
		}
		initAccountSQL := `
		INSERT INTO paper_account (id, initial_cash, current_cash, realized_pl, margin_multiplier, bootstrap_started_at, updated_at)
		VALUES (1, ?, ?, 0.0, ?, ?, CURRENT_TIMESTAMP);
		`
		bootTime := time.Now().UTC()
		if bootstrapDate != "" {
			if t, err := time.Parse("2006-01-02", bootstrapDate); err == nil {
				bootTime = t
			}
		}
		if _, err := db.Exec(initAccountSQL, initialCapital, initialCapital, marginMultiplier, bootTime); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to initialize paper account in sqlite: %w", err)
		}
	}

	return &SQLiteJournal{db: db}, nil
}

// Close closes the database connection.
func (s *SQLiteJournal) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// IsDuplicate checks if a signal_id has already been recorded.
func (s *SQLiteJournal) IsDuplicate(ctx context.Context, signalID string) (bool, *ExecutionRecord, error) {
	query := `
	SELECT id, signal_id, broker, order_id, client_order_id, symbol, action,
	       requested_shares, filled_shares, signal_price, fill_price, status,
	       strategy, submitted_at, filled_at, error_message, created_at
	FROM execution_journal
	WHERE signal_id = ?
	LIMIT 1;
	`
	var r ExecutionRecord
	var subAt, filledAt sql.NullTime
	var errMsg sql.NullString
	var orderID, clientOrderID, strategy sql.NullString

	err := s.db.QueryRowContext(ctx, query, signalID).Scan(
		&r.ID, &r.SignalID, &r.Broker, &orderID, &clientOrderID, &r.Symbol, &r.Action,
		&r.RequestedShares, &r.FilledShares, &r.SignalPrice, &r.FillPrice, &r.Status,
		&strategy, &subAt, &filledAt, &errMsg, &r.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("failed to check duplicate signal in sqlite: %w", err)
	}

	r.OrderID = orderID.String
	r.ClientOrderID = clientOrderID.String
	r.Strategy = strategy.String
	r.ErrorMessage = errMsg.String
	if subAt.Valid {
		r.SubmittedAt = subAt.Time
	}
	if filledAt.Valid {
		r.FilledAt = &filledAt.Time
	}

	return true, &r, nil
}

// RecordExecution inserts a new execution record into SQLite.
func (s *SQLiteJournal) RecordExecution(ctx context.Context, rec *ExecutionRecord) error {
	query := `
	INSERT INTO execution_journal (
		signal_id, broker, order_id, client_order_id, symbol, action,
		requested_shares, filled_shares, signal_price, fill_price,
		status, strategy, submitted_at, filled_at, error_message, raw_response, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
	`
	res, err := s.db.ExecContext(ctx, query,
		rec.SignalID, rec.Broker, rec.OrderID, rec.ClientOrderID, rec.Symbol, rec.Action,
		rec.RequestedShares, rec.FilledShares, rec.SignalPrice, rec.FillPrice,
		rec.Status, rec.Strategy, rec.SubmittedAt, rec.FilledAt, rec.ErrorMessage, rec.RawResponse, rec.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert execution record in sqlite: %w", err)
	}

	id, err := res.LastInsertId()
	if err == nil {
		rec.ID = id
	}
	return nil
}

// UpdateFill updates an existing record with fill status and details.
func (s *SQLiteJournal) UpdateFill(ctx context.Context, rec *ExecutionRecord) error {
	query := `
	UPDATE execution_journal
	SET order_id = ?, filled_shares = ?, fill_price = ?, status = ?,
	    filled_at = ?, error_message = ?, raw_response = ?
	WHERE signal_id = ?;
	`
	_, err := s.db.ExecContext(ctx, query,
		rec.OrderID, rec.FilledShares, rec.FillPrice, rec.Status,
		rec.FilledAt, rec.ErrorMessage, rec.RawResponse, rec.SignalID,
	)
	if err != nil {
		return fmt.Errorf("failed to update fill in sqlite: %w", err)
	}
	return nil
}

// GetRecentExecutions retrieves recent execution records.
func (s *SQLiteJournal) GetRecentExecutions(ctx context.Context, limit int) ([]*ExecutionRecord, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `
	SELECT id, signal_id, broker, order_id, client_order_id, symbol, action,
	       requested_shares, filled_shares, signal_price, fill_price, status,
	       strategy, submitted_at, filled_at, error_message, created_at
	FROM execution_journal
	ORDER BY id DESC
	LIMIT ?;
	`
	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query recent executions: %w", err)
	}
	defer rows.Close()

	records := make([]*ExecutionRecord, 0)
	for rows.Next() {
		var r ExecutionRecord
		var subAt, filledAt sql.NullTime
		var errMsg, orderID, clientOrderID, strategy sql.NullString

		if err := rows.Scan(
			&r.ID, &r.SignalID, &r.Broker, &orderID, &clientOrderID, &r.Symbol, &r.Action,
			&r.RequestedShares, &r.FilledShares, &r.SignalPrice, &r.FillPrice, &r.Status,
			&strategy, &subAt, &filledAt, &errMsg, &r.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan execution row: %w", err)
		}

		r.OrderID = orderID.String
		r.ClientOrderID = clientOrderID.String
		r.Strategy = strategy.String
		r.ErrorMessage = errMsg.String
		if subAt.Valid {
			r.SubmittedAt = subAt.Time
		}
		if filledAt.Valid {
			r.FilledAt = &filledAt.Time
		}
		records = append(records, &r)
	}
	return records, nil
}

// SetLastEquity persists the authoritative prior-session closing equity (e.g. Alpaca account.last_equity)
// so GetAccount computes true single-day equity change (equity - last_equity) without mixing
// multi-day realized P&L from closed trades with single-day MTM on open positions.
func (s *SQLiteJournal) SetLastEquity(ctx context.Context, lastEquity float64) error {
	if lastEquity <= 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE paper_account
		SET last_equity = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = 1;
	`, lastEquity)
	if err != nil {
		return fmt.Errorf("failed to update paper account last_equity: %w", err)
	}
	return nil
}

// GetAccount computes current paper account cash, equity, buying power, and P&L.
func (s *SQLiteJournal) GetAccount(ctx context.Context) (*broker.Account, error) {
	var currentCash, initialCash, marginMult, lastEquity float64
	err := s.db.QueryRowContext(ctx, `
		SELECT current_cash, initial_cash, margin_multiplier, COALESCE(last_equity, 0)
		FROM paper_account WHERE id = 1;
	`).Scan(&currentCash, &initialCash, &marginMult, &lastEquity)
	if err != nil {
		return nil, fmt.Errorf("failed to read paper account: %w", err)
	}
	if initialCash <= 0 {
		initialCash = 100000.0
	}

	startOfToday := getStartOfTodayCST()

	// Calculate current portfolio value, unrealized P&L, and day P&L from open positions
	rows, err := s.db.QueryContext(ctx, `
		SELECT symbol, shares, avg_entry_price, cost_basis, current_price, COALESCE(prev_close, 0), unrealized_pl, opened_at
		FROM paper_positions;
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query open paper positions: %w", err)
	}
	defer rows.Close()

	prevTradingClose := getPrevTradingCloseCST()

	var totalMarketVal, totalCostBasis, openPositionsDayPL float64
	for rows.Next() {
		var sym string
		var shares, avgEntry, costBasis, currPrice, prevClose, unPL float64
		var openedAt time.Time
		if err := rows.Scan(&sym, &shares, &avgEntry, &costBasis, &currPrice, &prevClose, &unPL, &openedAt); err == nil {
			mktVal := shares * currPrice
			totalMarketVal += mktVal
			totalCostBasis += costBasis

			// Baseline for day change:
			// If opened after the previous trading session close (e.g. over the weekend or today)
			// or prevClose is not available, baseline is avg entry price.
			baseline := prevClose
			if baseline <= 0 || !openedAt.Before(prevTradingClose) {
				baseline = avgEntry
			}
			if baseline > 0 {
				openPositionsDayPL += shares * (currPrice - baseline)
			}
		}
	}

	// Build canceled signal_id set so canceled orders never distort today's realized P&L
	canceledSigs := make(map[string]bool)
	cRows, cErr := s.db.QueryContext(ctx, `SELECT signal_id FROM execution_journal WHERE LOWER(status) = 'canceled';`)
	if cErr == nil {
		defer cRows.Close()
		for cRows.Next() {
			var cs string
			if err := cRows.Scan(&cs); err == nil && cs != "" {
				canceledSigs[strings.TrimSpace(cs)] = true
			}
		}
	}

	// Sum realized P&L from valid trades closed today (action = 'SELL' on or after startOfToday)
	var dayRealizedPL float64
	tradeRows, err := s.db.QueryContext(ctx, `
		SELECT realized_pl, COALESCE(signal_id, ''), timestamp
		FROM paper_trades
		WHERE action = 'SELL';
	`)
	if err == nil {
		defer tradeRows.Close()
		for tradeRows.Next() {
			var rpl float64
			var sigID string
			var tradeTime time.Time
			if err := tradeRows.Scan(&rpl, &sigID, &tradeTime); err == nil {
				sigTrim := strings.TrimSpace(sigID)
				if canceledSigs[sigTrim] || strings.HasPrefix(sigTrim, "sig-test-") {
					continue
				}
				if !tradeTime.Before(startOfToday) {
					dayRealizedPL += rpl
				}
			}
		}
	}

	equity := currentCash + totalMarketVal
	totalUnrealizedPL := totalMarketVal - totalCostBasis
	realizedPL := (currentCash + totalCostBasis) - initialCash
	buyingPower := currentCash * marginMult
	if buyingPower < 0 {
		buyingPower = 0
	}

	// Authoritative true daily equity change:
	// When last_equity (prior RTH close equity from Alpaca) is available, DayPL = equity - last_equity.
	// This avoids conflating multi-day realized P&L on closed legs with single-day MTM on open legs.
	totalDayPL := openPositionsDayPL + dayRealizedPL
	if lastEquity > 0 {
		totalDayPL = equity - lastEquity
	}

	acctID := s.accountID
	if acctID == "" {
		acctID = os.Getenv("ACCOUNT_ID")
		if acctID == "" {
			acctID = "678a607c-193f-407a-aa27-e5dd864db293"
		}
	}
	acctNum := s.accountNumber
	if acctNum == "" {
		acctNum = os.Getenv("ACCOUNT_NUMBER")
		if acctNum == "" {
			acctNum = "PA3HW1A38DR2"
		}
	}

	return &broker.Account{
		ID:             acctID,
		AccountNumber:  acctNum,
		Status:         "ACTIVE",
		Currency:       "USD",
		Cash:           currentCash,
		BuyingPower:    buyingPower,
		Equity:         equity,
		LastEquity:     lastEquity,
		PortfolioValue: totalMarketVal,
		RealizedPL:     realizedPL,
		UnrealizedPL:   totalUnrealizedPL,
		DayPL:          totalDayPL,
		DayRealizedPL:  dayRealizedPL,
		TradingBlocked: false,
	}, nil
}

// GetPosition retrieves the open position for a given symbol.
func (s *SQLiteJournal) GetPosition(ctx context.Context, symbol string) (*broker.Position, error) {
	sym := strings.ToUpper(strings.TrimSpace(symbol))
	var shares, avgEntry, costBasis, currPrice, prevClose, unPL float64
	var openedAt, updatedAt time.Time

	err := s.db.QueryRowContext(ctx, `
		SELECT shares, avg_entry_price, cost_basis, current_price, COALESCE(prev_close, 0), unrealized_pl, opened_at, updated_at
		FROM paper_positions
		WHERE symbol = ?;
	`, sym).Scan(&shares, &avgEntry, &costBasis, &currPrice, &prevClose, &unPL, &openedAt, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, nil // No position held
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query position for %s: %w", sym, err)
	}

	mktVal := shares * currPrice
	unrealizedPL := mktVal - costBasis
	unrealizedPLPct := 0.0
	if costBasis > 0 {
		unrealizedPLPct = (unrealizedPL / costBasis) * 100.0
	}
	dayChange := 0.0
	dayChangePct := 0.0
	dayPL := 0.0

	prevTradingClose := getPrevTradingCloseCST()
	baseline := prevClose
	if baseline <= 0 || !openedAt.Before(prevTradingClose) {
		baseline = avgEntry
	}
	if baseline > 0 {
		dayChange = currPrice - baseline
		dayChangePct = (dayChange / baseline) * 100.0
		dayPL = shares * dayChange
	}

	return &broker.Position{
		Symbol:          sym,
		Qty:             shares,
		AvgEntryPrice:   avgEntry,
		BrokerFillPrice: avgEntry,
		CurrentPrice:    currPrice,
		PrevClosePrice:  prevClose,
		DayChange:       dayChange,
		DayChangePct:    dayChangePct,
		DayPL:           dayPL,
		MarketValue:     mktVal,
		CostBasis:       costBasis,
		UnrealizedPL:    unrealizedPL,
		UnrealizedPLPct: unrealizedPLPct,
		OpenedAt:        openedAt,
		UpdatedAt:       updatedAt,
	}, nil
}

// GetAllPositions retrieves all open paper positions.
func (s *SQLiteJournal) GetAllPositions(ctx context.Context) ([]*broker.Position, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT symbol, shares, avg_entry_price, cost_basis, current_price, COALESCE(prev_close, 0), unrealized_pl, opened_at, updated_at
		FROM paper_positions
		WHERE shares > 0
		ORDER BY symbol ASC;
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query all paper positions: %w", err)
	}
	defer rows.Close()

	prevTradingClose := getPrevTradingCloseCST()
	list := make([]*broker.Position, 0)
	for rows.Next() {
		var sym string
		var shares, avgEntry, costBasis, currPrice, prevClose, unPL float64
		var openedAt, updatedAt time.Time
		if err := rows.Scan(&sym, &shares, &avgEntry, &costBasis, &currPrice, &prevClose, &unPL, &openedAt, &updatedAt); err != nil {
			continue
		}
		mktVal := shares * currPrice
		calcPL := mktVal - costBasis
		plPct := 0.0
		if costBasis > 0 {
			plPct = (calcPL / costBasis) * 100.0
		}
		dayChange := 0.0
		dayChangePct := 0.0
		dayPL := 0.0

		baseline := prevClose
		if baseline <= 0 || !openedAt.Before(prevTradingClose) {
			baseline = avgEntry
		}
		if baseline > 0 {
			dayChange = currPrice - baseline
			dayChangePct = (dayChange / baseline) * 100.0
			dayPL = shares * dayChange
		}

		list = append(list, &broker.Position{
			Symbol:          sym,
			Qty:             shares,
			AvgEntryPrice:   avgEntry,
			BrokerFillPrice: avgEntry,
			CurrentPrice:    currPrice,
			PrevClosePrice:  prevClose,
			DayChange:       dayChange,
			DayChangePct:    dayChangePct,
			DayPL:           dayPL,
			MarketValue:     mktVal,
			CostBasis:       costBasis,
			UnrealizedPL:    calcPL,
			UnrealizedPLPct: plPct,
			OpenedAt:        openedAt,
			UpdatedAt:       updatedAt,
		})
	}
	return list, nil
}

// ApplyBuy applies a confirmed BUY to the paper portfolio and deducts cash.
func (s *SQLiteJournal) ApplyBuy(ctx context.Context, symbol string, shares float64, price float64, signalID string) error {
	sym := strings.ToUpper(strings.TrimSpace(symbol))
	cost := shares * price

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Deduct cash from account
	if _, err := tx.ExecContext(ctx, `
		UPDATE paper_account
		SET current_cash = current_cash - ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = 1;
	`, cost); err != nil {
		return fmt.Errorf("failed to deduct cash: %w", err)
	}

	// 2. Check if existing position exists
	var existingShares, existingCost float64
	row := tx.QueryRowContext(ctx, `SELECT shares, cost_basis FROM paper_positions WHERE symbol = ?;`, sym)
	err = row.Scan(&existingShares, &existingCost)

	now := time.Now().UTC()
	if err == sql.ErrNoRows {
		// New position
		_, err = tx.ExecContext(ctx, `
			INSERT INTO paper_positions (symbol, shares, avg_entry_price, cost_basis, current_price, unrealized_pl, opened_at, updated_at)
			VALUES (?, ?, ?, ?, ?, 0.0, ?, ?);
		`, sym, shares, price, cost, price, now, now)
		if err != nil {
			return fmt.Errorf("failed to insert new position: %w", err)
		}
	} else if err == nil {
		// Add to existing position
		totalShares := existingShares + shares
		totalCost := existingCost + cost
		newAvg := totalCost / totalShares
		_, err = tx.ExecContext(ctx, `
			UPDATE paper_positions
			SET shares = ?, avg_entry_price = ?, cost_basis = ?, current_price = ?, unrealized_pl = 0.0, updated_at = ?
			WHERE symbol = ?;
		`, totalShares, newAvg, totalCost, price, now, sym)
		if err != nil {
			return fmt.Errorf("failed to update existing position: %w", err)
		}
	} else {
		return fmt.Errorf("error reading position: %w", err)
	}

	// 3. Log trade to paper_trades
	_, _ = tx.ExecContext(ctx, `
		INSERT INTO paper_trades (symbol, action, shares, price, realized_pl, signal_id, timestamp)
		VALUES (?, 'BUY', ?, ?, 0.0, ?, ?);
	`, sym, shares, price, signalID, now)

	return tx.Commit()
}

// ApplySell applies a confirmed SELL to the paper portfolio, closes or reduces the position,
// realizes profit/loss, and credits cash.
func (s *SQLiteJournal) ApplySell(ctx context.Context, symbol string, shares float64, price float64, signalID string) (float64, error) {
	sym := strings.ToUpper(strings.TrimSpace(symbol))

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var existingShares, avgEntry, existingCost float64
	err = tx.QueryRowContext(ctx, `
		SELECT shares, avg_entry_price, cost_basis FROM paper_positions WHERE symbol = ?;
	`, sym).Scan(&existingShares, &avgEntry, &existingCost)
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("no open position found for %s", sym)
	}
	if err != nil {
		return 0, fmt.Errorf("failed to read position for %s: %w", sym, err)
	}

	sharesToSell := shares
	if sharesToSell > existingShares {
		sharesToSell = existingShares
	}

	costOfSold := sharesToSell * avgEntry
	proceeds := sharesToSell * price
	realizedPL := proceeds - costOfSold

	now := time.Now().UTC()

	// 1. Credit cash & add realized P&L
	if _, err := tx.ExecContext(ctx, `
		UPDATE paper_account
		SET current_cash = current_cash + ?, realized_pl = realized_pl + ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = 1;
	`, proceeds, realizedPL); err != nil {
		return 0, fmt.Errorf("failed to update paper account cash/p&l: %w", err)
	}

	// 2. Update or delete position
	remainingShares := existingShares - sharesToSell
	if remainingShares <= 0.0001 {
		// Fully closed
		if _, err := tx.ExecContext(ctx, `DELETE FROM paper_positions WHERE symbol = ?;`, sym); err != nil {
			return 0, fmt.Errorf("failed to delete closed position: %w", err)
		}
	} else {
		// Partially closed
		remainingCost := existingCost - costOfSold
		if _, err := tx.ExecContext(ctx, `
			UPDATE paper_positions
			SET shares = ?, cost_basis = ?, current_price = ?, unrealized_pl = (? * current_price) - ?, updated_at = ?
			WHERE symbol = ?;
		`, remainingShares, remainingCost, price, remainingShares, remainingCost, now, sym); err != nil {
			return 0, fmt.Errorf("failed to reduce position: %w", err)
		}
	}

	// 3. Log trade
	_, _ = tx.ExecContext(ctx, `
		INSERT INTO paper_trades (symbol, action, shares, price, realized_pl, signal_id, timestamp)
		VALUES (?, 'SELL', ?, ?, ?, ?, ?);
	`, sym, sharesToSell, price, realizedPL, signalID, now)

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return realizedPL, nil
}

// UpdateMarketPrice updates the latest mark price and optional previous close for an open position.
func (s *SQLiteJournal) UpdateMarketPrice(ctx context.Context, symbol string, price float64, prevClose float64) error {
	sym := strings.ToUpper(strings.TrimSpace(symbol))
	if prevClose > 0 {
		_, err := s.db.ExecContext(ctx, `
			UPDATE paper_positions
			SET current_price = ?, prev_close = ?, unrealized_pl = (shares * ?) - cost_basis, updated_at = CURRENT_TIMESTAMP
			WHERE symbol = ?;
		`, price, prevClose, price, sym)
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE paper_positions
		SET current_price = ?, unrealized_pl = (shares * ?) - cost_basis, updated_at = CURRENT_TIMESTAMP
		WHERE symbol = ?;
	`, price, price, sym)
	return err
}

// GetRecentTrades retrieves the recent closed/executed trades log.
func (s *SQLiteJournal) GetRecentTrades(ctx context.Context, limit int) ([]*PaperTrade, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, symbol, action, shares, price, realized_pl, signal_id, timestamp
		FROM paper_trades
		ORDER BY id DESC
		LIMIT ?;
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query paper trades: %w", err)
	}
	defer rows.Close()

	trades := make([]*PaperTrade, 0)
	for rows.Next() {
		var t PaperTrade
		var sigID sql.NullString
		if err := rows.Scan(&t.ID, &t.Symbol, &t.Action, &t.Shares, &t.Price, &t.RealizedPL, &sigID, &t.Timestamp); err != nil {
			continue
		}
		t.SignalID = sigID.String
		trades = append(trades, &t)
	}
	return trades, nil
}

// ReconcilePositions synchronizes paper portfolio positions and historical execution fills with authoritative broker state.
// If replace is true, any positions in SQLite not in the provided list are removed.
func (s *SQLiteJournal) ReconcilePositions(ctx context.Context, positions []*broker.ReconcilePositionItem, orders []*broker.ReconcileOrderFillItem, cash float64, replace bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if cash > 0 {
		if _, err := tx.ExecContext(ctx, `
			UPDATE paper_account
			SET current_cash = ?, updated_at = CURRENT_TIMESTAMP
			WHERE id = 1;
		`, cash); err != nil {
			return fmt.Errorf("failed to update paper account cash: %w", err)
		}
	}

	// 1. Process broker order execution fills if provided
	for _, o := range orders {
		if o == nil || o.FillPrice <= 0 {
			continue
		}
		sym := strings.ToUpper(strings.TrimSpace(o.Symbol))
		act := strings.ToUpper(strings.TrimSpace(o.Action))
		matched := false

		var parsedTS time.Time
		hasTS := false
		if tsStr := strings.TrimSpace(o.Timestamp); tsStr != "" {
			if t, pErr := time.Parse(time.RFC3339Nano, tsStr); pErr == nil {
				parsedTS = t.UTC()
				hasTS = true
			} else if t, pErr := time.Parse(time.RFC3339, tsStr); pErr == nil {
				parsedTS = t.UTC()
				hasTS = true
			}
		}

		// A. Try exact match by order_id or client_order_id or signal_id in execution_journal
		if o.OrderID != "" || o.ClientOrderID != "" {
			var execID int64
			var execSigID string
			err := tx.QueryRowContext(ctx, `
				SELECT id, signal_id FROM execution_journal
				WHERE (order_id = ? AND order_id != '')
				   OR (client_order_id = ? AND client_order_id != '')
				   OR (signal_id = ? AND signal_id != '')
				ORDER BY id DESC LIMIT 1;
			`, o.OrderID, o.ClientOrderID, o.ClientOrderID).Scan(&execID, &execSigID)
			if err == nil && execID > 0 {
				matched = true
				if o.Shares > 0 {
					_, _ = tx.ExecContext(ctx, `
						UPDATE execution_journal
						SET fill_price = ?,
						    filled_shares = ?,
						    requested_shares = CASE WHEN requested_shares < ? THEN ? ELSE requested_shares END,
						    status = 'filled'
						WHERE id = ?;
					`, o.FillPrice, o.Shares, o.Shares, o.Shares, execID)
				} else {
					_, _ = tx.ExecContext(ctx, `
						UPDATE execution_journal
						SET fill_price = ?, status = 'filled'
						WHERE id = ?;
					`, o.FillPrice, execID)
				}

				if execSigID != "" {
					if o.Shares > 0 && o.RealizedPL != nil {
						_, _ = tx.ExecContext(ctx, `
							UPDATE paper_trades
							SET shares = ?, price = ?, realized_pl = ?
							WHERE signal_id = ?;
						`, o.Shares, o.FillPrice, *o.RealizedPL, execSigID)
					} else if o.Shares > 0 {
						_, _ = tx.ExecContext(ctx, `
							UPDATE paper_trades
							SET shares = ?, price = ?
							WHERE signal_id = ?;
						`, o.Shares, o.FillPrice, execSigID)
					} else if o.RealizedPL != nil {
						_, _ = tx.ExecContext(ctx, `
							UPDATE paper_trades
							SET price = ?, realized_pl = ?
							WHERE signal_id = ?;
						`, o.FillPrice, *o.RealizedPL, execSigID)
					} else {
						_, _ = tx.ExecContext(ctx, `
							UPDATE paper_trades
							SET price = ?
							WHERE signal_id = ?;
						`, o.FillPrice, execSigID)
					}
					if hasTS {
						_, _ = tx.ExecContext(ctx, `UPDATE paper_trades SET timestamp = ? WHERE signal_id = ?;`, parsedTS, execSigID)
					}
				}
			}
		}

		// A2. Try direct signal_id match in paper_trades (via order_id or client_order_id)
		if !matched && (o.OrderID != "" || o.ClientOrderID != "") {
			var tradeID int64
			err := tx.QueryRowContext(ctx, `
				SELECT id FROM paper_trades
				WHERE (signal_id = ? AND signal_id != '')
				   OR (signal_id = ? AND signal_id != '')
				ORDER BY id DESC LIMIT 1;
			`, o.OrderID, o.ClientOrderID).Scan(&tradeID)
			if err == nil && tradeID > 0 {
				matched = true
				if o.RealizedPL != nil {
					_, _ = tx.ExecContext(ctx, `
						UPDATE paper_trades
						SET shares = ?, price = ?, realized_pl = ?
						WHERE id = ?;
					`, o.Shares, o.FillPrice, *o.RealizedPL, tradeID)
				} else {
					_, _ = tx.ExecContext(ctx, `
						UPDATE paper_trades
						SET shares = ?, price = ?
						WHERE id = ?;
					`, o.Shares, o.FillPrice, tradeID)
				}
				if hasTS {
					_, _ = tx.ExecContext(ctx, `UPDATE paper_trades SET timestamp = ? WHERE id = ?;`, parsedTS, tradeID)
				}
			}
		}

		// A3. Explicit historical broker fill backfill (when Upsert is requested and no exact ID match exists)
		if !matched && o.Upsert && sym != "" && (act == "BUY" || act == "SELL") && o.Shares > 0 {
			sigID := strings.TrimSpace(o.ClientOrderID)
			if sigID == "" {
				sigID = strings.TrimSpace(o.OrderID)
			}
			rplVal := 0.0
			if o.RealizedPL != nil {
				rplVal = *o.RealizedPL
			}
			tsVal := time.Now().UTC()
			if hasTS {
				tsVal = parsedTS
			}
			if _, iErr := tx.ExecContext(ctx, `
				INSERT INTO paper_trades (symbol, action, shares, price, realized_pl, signal_id, timestamp)
				VALUES (?, ?, ?, ?, ?, ?, ?);
			`, sym, act, o.Shares, o.FillPrice, rplVal, sigID, tsVal); iErr != nil {
				return fmt.Errorf("failed to backfill paper_trade for %s (%s): %w", sym, sigID, iErr)
			}
			matched = true
		}

		// B. Fallback: match latest trade by symbol and action
		if !matched && sym != "" && act != "" && o.Shares > 0 {
			var tradeID int64
			var tradeSigID string
			_ = tx.QueryRowContext(ctx, `
				SELECT id, signal_id FROM paper_trades
				WHERE symbol = ? AND action = ?
				ORDER BY id DESC LIMIT 1;
			`, sym, act).Scan(&tradeID, &tradeSigID)
			if tradeID > 0 {
				matched = true
				if o.RealizedPL != nil {
					_, _ = tx.ExecContext(ctx, `
						UPDATE paper_trades
						SET shares = ?, price = ?, realized_pl = ?
						WHERE id = ?;
					`, o.Shares, o.FillPrice, *o.RealizedPL, tradeID)
				} else {
					_, _ = tx.ExecContext(ctx, `
						UPDATE paper_trades
						SET shares = ?, price = ?
						WHERE id = ?;
					`, o.Shares, o.FillPrice, tradeID)
				}

				if tradeSigID != "" {
					_, _ = tx.ExecContext(ctx, `
						UPDATE execution_journal
						SET filled_shares = ?, fill_price = ?, status = 'filled'
						WHERE signal_id = ?;
					`, o.Shares, o.FillPrice, tradeSigID)
				} else {
					_, _ = tx.ExecContext(ctx, `
						UPDATE execution_journal
						SET filled_shares = ?, fill_price = ?, status = 'filled'
						WHERE id = (
							SELECT id FROM execution_journal
							WHERE symbol = ? AND action = ?
							ORDER BY id DESC LIMIT 1
						);
					`, o.Shares, o.FillPrice, sym, act)
				}
			}
		}
	}

	activeSymbols := make(map[string]bool)
	now := time.Now().UTC()

	// 2. Reconcile active open positions
	for _, p := range positions {
		sym := strings.ToUpper(strings.TrimSpace(p.Symbol))
		if sym == "" {
			continue
		}
		if p.Shares <= 0 {
			if _, err := tx.ExecContext(ctx, `DELETE FROM paper_positions WHERE symbol = ?;`, sym); err != nil {
				return fmt.Errorf("failed to delete 0-share position %s: %w", sym, err)
			}
			continue
		}

		activeSymbols[sym] = true
		costBasis := p.CostBasis
		if costBasis <= 0 {
			costBasis = p.Shares * p.AvgEntryPrice
		}
		currPrice := p.CurrentPrice
		if currPrice <= 0 {
			currPrice = p.AvgEntryPrice
		}
		unrealizedPL := (p.Shares * currPrice) - costBasis

		// Check if position exists
		var exists int
		_ = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM paper_positions WHERE symbol = ?;`, sym).Scan(&exists)

		if exists > 0 {
			if p.PrevClose > 0 {
				_, err = tx.ExecContext(ctx, `
					UPDATE paper_positions
					SET shares = ?, avg_entry_price = ?, cost_basis = ?, current_price = ?,
					    prev_close = ?, unrealized_pl = ?, updated_at = ?
					WHERE symbol = ?;
				`, p.Shares, p.AvgEntryPrice, costBasis, currPrice, p.PrevClose, unrealizedPL, now, sym)
			} else {
				_, err = tx.ExecContext(ctx, `
					UPDATE paper_positions
					SET shares = ?, avg_entry_price = ?, cost_basis = ?, current_price = ?,
					    unrealized_pl = ?, updated_at = ?
					WHERE symbol = ?;
				`, p.Shares, p.AvgEntryPrice, costBasis, currPrice, unrealizedPL, now, sym)
			}
		} else {
			_, err = tx.ExecContext(ctx, `
				INSERT INTO paper_positions (symbol, shares, avg_entry_price, cost_basis, current_price, prev_close, unrealized_pl, opened_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
			`, sym, p.Shares, p.AvgEntryPrice, costBasis, currPrice, p.PrevClose, unrealizedPL, now, now)
		}

		if err != nil {
			return fmt.Errorf("failed to reconcile position for %s: %w", sym, err)
		}

		// Also update latest execution_journal BUY record to reflect true broker fill price
		_, _ = tx.ExecContext(ctx, `
			UPDATE execution_journal
			SET fill_price = ?, status = 'filled'
			WHERE id = (
				SELECT id FROM execution_journal
				WHERE symbol = ? AND action = 'BUY'
				ORDER BY id DESC LIMIT 1
			);
		`, p.AvgEntryPrice, sym)

		// And paper_trades
		_, _ = tx.ExecContext(ctx, `
			UPDATE paper_trades
			SET price = ?
			WHERE id = (
				SELECT id FROM paper_trades
				WHERE symbol = ? AND action = 'BUY'
				ORDER BY id DESC LIMIT 1
			);
		`, p.AvgEntryPrice, sym)
	}

	if replace {
		if len(activeSymbols) == 0 {
			if _, err := tx.ExecContext(ctx, `DELETE FROM paper_positions;`); err != nil {
				return fmt.Errorf("failed to clear positions: %w", err)
			}
			if len(orders) == 0 {
				_, _ = tx.ExecContext(ctx, `DELETE FROM paper_trades;`)
				_, _ = tx.ExecContext(ctx, `DELETE FROM execution_journal;`)
			}
		} else {
			rows, err := tx.QueryContext(ctx, `SELECT symbol FROM paper_positions;`)
			if err != nil {
				return fmt.Errorf("failed to query existing symbols: %w", err)
			}
			defer rows.Close()

			var toDelete []string
			for rows.Next() {
				var s string
				if err := rows.Scan(&s); err == nil {
					if !activeSymbols[s] {
						toDelete = append(toDelete, s)
					}
				}
			}
			rows.Close()

			for _, s := range toDelete {
				if _, err := tx.ExecContext(ctx, `DELETE FROM paper_positions WHERE symbol = ?;`, s); err != nil {
					return fmt.Errorf("failed to delete non-active position %s: %w", s, err)
				}
			}
		}
	}

	// 3. Recompute realized_pl for all SELL trades in paper_trades based on chronological prior BUY fill prices
	tRows, err := tx.QueryContext(ctx, `
		SELECT id, symbol, action, shares, price, COALESCE(signal_id, ''), timestamp
		FROM paper_trades
		ORDER BY id ASC;
	`)
	if err == nil {
		type tradeRec struct {
			id       int64
			symbol   string
			action   string
			shares   float64
			price    float64
			signalID string
			ts       time.Time
		}
		canceledBuys := map[string]bool{
			"sig-buy-LABD-202610281500-250": true,
			"sig-buy-SPXS-202610281500-100": true,
			"sig-buy-TZA-202610281500-50":   true,
		}
		var allTrades []tradeRec
		for tRows.Next() {
			var r tradeRec
			if err := tRows.Scan(&r.id, &r.symbol, &r.action, &r.shares, &r.price, &r.signalID, &r.ts); err == nil {
				r.symbol = strings.ToUpper(strings.TrimSpace(r.symbol))
				r.action = strings.ToUpper(strings.TrimSpace(r.action))
				r.signalID = strings.TrimSpace(r.signalID)
				allTrades = append(allTrades, r)
			}
		}
		tRows.Close()

		sort.Slice(allTrades, func(i, j int) bool {
			if allTrades[i].ts.Equal(allTrades[j].ts) {
				return allTrades[i].id < allTrades[j].id
			}
			return allTrades[i].ts.Before(allTrades[j].ts)
		})

		lastBuyPriceBySym := make(map[string]float64)
		for _, tr := range allTrades {
			if strings.HasPrefix(tr.signalID, "sig-test-") || canceledBuys[tr.signalID] {
				continue
			}
			if tr.action == "BUY" && tr.price > 0 {
				lastBuyPriceBySym[tr.symbol] = tr.price
			} else if tr.action == "SELL" {
				if buyPrice, ok := lastBuyPriceBySym[tr.symbol]; ok && buyPrice > 0 {
					rpl := tr.shares * (tr.price - buyPrice)
					_, _ = tx.ExecContext(ctx, `UPDATE paper_trades SET realized_pl = ? WHERE id = ?;`, rpl, tr.id)
				}
			}
		}
	}

	// 4. Update paper_account realized_pl to match exact authoritative accounting
	if cash > 0 {
		var initCash float64
		_ = tx.QueryRowContext(ctx, `SELECT initial_cash FROM paper_account WHERE id = 1;`).Scan(&initCash)
		if initCash <= 0 {
			initCash = 100000.0
		}
		var openCostBasis float64
		_ = tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(cost_basis), 0) FROM paper_positions;`).Scan(&openCostBasis)
		authoritativeRealizedPL := (cash + openCostBasis) - initCash
		_, _ = tx.ExecContext(ctx, `UPDATE paper_account SET realized_pl = ? WHERE id = 1;`, authoritativeRealizedPL)
	}

	return tx.Commit()
}

// SyncZeroLinesAndRepaints upserts active zero-line pair anchors and appends
// deduplicated position repaint / zero-line breach events into SQLite.
func (s *SQLiteJournal) SyncZeroLinesAndRepaints(
	ctx context.Context,
	zeroLines []*PositionZeroLine,
	repaints []*PositionRepaint,
	replaceOpen bool,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin zero-line sync tx: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	activeSyms := make(map[string]bool)

	for _, zl := range zeroLines {
		if zl == nil || strings.TrimSpace(zl.ActiveSymbol) == "" {
			continue
		}
		sym := strings.ToUpper(strings.TrimSpace(zl.ActiveSymbol))
		psym := strings.ToUpper(strings.TrimSpace(zl.PartnerSymbol))
		activeSyms[sym] = true
		upd := zl.UpdatedAt
		if upd.IsZero() {
			upd = now
		}
		state := strings.ToUpper(strings.TrimSpace(zl.State))
		if state == "" {
			state = "OPEN"
		}
		status := strings.ToUpper(strings.TrimSpace(zl.ZeroLineStatus))
		if status == "" {
			status = "ABOVE_ZERO"
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO position_zero_lines (
				active_symbol, pair_key, entry_signal_id, partner_symbol, armed_date, pivot_label,
				active_zero_price, partner_zero_price, active_entry_price, partner_entry_price,
				active_live_price, partner_live_price, active_vs_zero_pct, partner_vs_zero_pct,
				active_linear_state, partner_linear_state, flip_count, zero_line_status,
				state, repaint_count, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(active_symbol) DO UPDATE SET
				pair_key = excluded.pair_key,
				entry_signal_id = CASE WHEN excluded.entry_signal_id != '' THEN excluded.entry_signal_id ELSE position_zero_lines.entry_signal_id END,
				partner_symbol = excluded.partner_symbol,
				armed_date = CASE WHEN excluded.armed_date != '' THEN excluded.armed_date ELSE position_zero_lines.armed_date END,
				pivot_label = CASE WHEN excluded.pivot_label != '' THEN excluded.pivot_label ELSE position_zero_lines.pivot_label END,
				active_zero_price = CASE WHEN excluded.active_zero_price > 0 THEN excluded.active_zero_price ELSE position_zero_lines.active_zero_price END,
				partner_zero_price = CASE WHEN excluded.partner_zero_price > 0 THEN excluded.partner_zero_price ELSE position_zero_lines.partner_zero_price END,
				active_entry_price = CASE WHEN excluded.active_entry_price > 0 THEN excluded.active_entry_price ELSE position_zero_lines.active_entry_price END,
				partner_entry_price = CASE WHEN excluded.partner_entry_price > 0 THEN excluded.partner_entry_price ELSE position_zero_lines.partner_entry_price END,
				active_live_price = excluded.active_live_price,
				partner_live_price = excluded.partner_live_price,
				active_vs_zero_pct = excluded.active_vs_zero_pct,
				partner_vs_zero_pct = excluded.partner_vs_zero_pct,
				active_linear_state = excluded.active_linear_state,
				partner_linear_state = excluded.partner_linear_state,
				flip_count = excluded.flip_count,
				zero_line_status = excluded.zero_line_status,
				state = excluded.state,
				repaint_count = excluded.repaint_count,
				updated_at = excluded.updated_at;
		`,
			sym, zl.PairKey, zl.EntrySignalID, psym, zl.ArmedDate, zl.PivotLabel,
			zl.ActiveZeroPrice, zl.PartnerZeroPrice, zl.ActiveEntryPrice, zl.PartnerEntryPrice,
			zl.ActiveLivePrice, zl.PartnerLivePrice, zl.ActiveVsZeroPct, zl.PartnerVsZeroPct,
			zl.ActiveLinearState, zl.PartnerLinearState, zl.FlipCount, status,
			state, zl.RepaintCount, upd,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert zero line for %s: %w", sym, err)
		}
	}

	if replaceOpen {
		rows, err := tx.QueryContext(ctx, `SELECT active_symbol FROM position_zero_lines;`)
		if err == nil {
			var stale []string
			for rows.Next() {
				var sSym string
				if rows.Scan(&sSym) == nil && !activeSyms[sSym] {
					stale = append(stale, sSym)
				}
			}
			rows.Close()
			for _, sSym := range stale {
				_, _ = tx.ExecContext(ctx, `DELETE FROM position_zero_lines WHERE active_symbol = ?;`, sSym)
			}
		}
	}

	for _, rp := range repaints {
		if rp == nil || strings.TrimSpace(rp.Symbol) == "" || strings.TrimSpace(rp.BarDate) == "" {
			continue
		}
		det := rp.DetectedAt
		if det.IsZero() {
			det = now
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO position_repaints (
				pair_key, symbol, partner_symbol, bar_date, event_type,
				orig_pivot_price, new_repaint_price, active_live_price, partner_live_price,
				active_vs_zero_pct, partner_vs_zero_pct, partner_linear_state,
				action_taken, note, detected_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(pair_key, symbol, bar_date, event_type) DO UPDATE SET
				new_repaint_price = CASE WHEN excluded.new_repaint_price > 0 THEN excluded.new_repaint_price ELSE position_repaints.new_repaint_price END,
				active_live_price = excluded.active_live_price,
				partner_live_price = excluded.partner_live_price,
				active_vs_zero_pct = excluded.active_vs_zero_pct,
				partner_vs_zero_pct = excluded.partner_vs_zero_pct,
				partner_linear_state = excluded.partner_linear_state,
				action_taken = excluded.action_taken,
				note = CASE WHEN excluded.note != '' THEN excluded.note ELSE position_repaints.note END;
		`,
			rp.PairKey, strings.ToUpper(strings.TrimSpace(rp.Symbol)),
			strings.ToUpper(strings.TrimSpace(rp.PartnerSymbol)),
			rp.BarDate, strings.ToUpper(strings.TrimSpace(rp.EventType)),
			rp.OrigPivotPrice, rp.NewRepaintPrice, rp.ActiveLivePrice, rp.PartnerLivePrice,
			rp.ActiveVsZeroPct, rp.PartnerVsZeroPct, rp.PartnerLinearState,
			rp.ActionTaken, rp.Note, det,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert position repaint for %s: %w", rp.Symbol, err)
		}
	}

	return tx.Commit()
}

// GetOpenZeroLines returns all active position zero-line anchors ordered by symbol.
func (s *SQLiteJournal) GetOpenZeroLines(ctx context.Context) ([]*PositionZeroLine, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT active_symbol, pair_key, entry_signal_id, partner_symbol, armed_date, pivot_label,
		       active_zero_price, partner_zero_price, active_entry_price, partner_entry_price,
		       active_live_price, partner_live_price, active_vs_zero_pct, partner_vs_zero_pct,
		       active_linear_state, partner_linear_state, flip_count, zero_line_status,
		       state, repaint_count, updated_at
		FROM position_zero_lines
		WHERE state = 'OPEN'
		ORDER BY active_symbol ASC;
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*PositionZeroLine, 0)
	for rows.Next() {
		var z PositionZeroLine
		if err := rows.Scan(
			&z.ActiveSymbol, &z.PairKey, &z.EntrySignalID, &z.PartnerSymbol, &z.ArmedDate, &z.PivotLabel,
			&z.ActiveZeroPrice, &z.PartnerZeroPrice, &z.ActiveEntryPrice, &z.PartnerEntryPrice,
			&z.ActiveLivePrice, &z.PartnerLivePrice, &z.ActiveVsZeroPct, &z.PartnerVsZeroPct,
			&z.ActiveLinearState, &z.PartnerLinearState, &z.FlipCount, &z.ZeroLineStatus,
			&z.State, &z.RepaintCount, &z.UpdatedAt,
		); err == nil {
			out = append(out, &z)
		}
	}
	return out, nil
}

// GetRecentPositionRepaints returns the most recent position repaint & zero-line events.
func (s *SQLiteJournal) GetRecentPositionRepaints(ctx context.Context, limit int) ([]*PositionRepaint, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, pair_key, symbol, partner_symbol, bar_date, event_type,
		       orig_pivot_price, new_repaint_price, active_live_price, partner_live_price,
		       active_vs_zero_pct, partner_vs_zero_pct, partner_linear_state,
		       action_taken, note, detected_at
		FROM position_repaints
		ORDER BY bar_date DESC, id DESC
		LIMIT ?;
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]*PositionRepaint, 0)
	for rows.Next() {
		var r PositionRepaint
		if err := rows.Scan(
			&r.ID, &r.PairKey, &r.Symbol, &r.PartnerSymbol, &r.BarDate, &r.EventType,
			&r.OrigPivotPrice, &r.NewRepaintPrice, &r.ActiveLivePrice, &r.PartnerLivePrice,
			&r.ActiveVsZeroPct, &r.PartnerVsZeroPct, &r.PartnerLinearState,
			&r.ActionTaken, &r.Note, &r.DetectedAt,
		); err == nil {
			out = append(out, &r)
		}
	}
	return out, nil
}
