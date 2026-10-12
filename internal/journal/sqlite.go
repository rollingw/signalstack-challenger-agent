package journal

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
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
	`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize sqlite schema: %w", err)
	}

	// Migration: ensure prev_close column exists on paper_positions
	_, _ = db.Exec("ALTER TABLE paper_positions ADD COLUMN prev_close REAL DEFAULT 0;")

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

// GetAccount computes current paper account cash, equity, buying power, and P&L.
func (s *SQLiteJournal) GetAccount(ctx context.Context) (*broker.Account, error) {
	var currentCash, initialCash, marginMult float64
	err := s.db.QueryRowContext(ctx, `
		SELECT current_cash, initial_cash, margin_multiplier
		FROM paper_account WHERE id = 1;
	`).Scan(&currentCash, &initialCash, &marginMult)
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

	var totalMarketVal, totalCostBasis, totalDayPL float64
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
				totalDayPL += shares * (currPrice - baseline)
			}
		}
	}

	// Add realized P&L from trades closed today (action = 'SELL' on or after startOfToday)
	tradeRows, err := s.db.QueryContext(ctx, `
		SELECT realized_pl, timestamp
		FROM paper_trades
		WHERE action = 'SELL';
	`)
	if err == nil {
		defer tradeRows.Close()
		for tradeRows.Next() {
			var rpl float64
			var tradeTime time.Time
			if err := tradeRows.Scan(&rpl, &tradeTime); err == nil {
				if !tradeTime.Before(startOfToday) {
					totalDayPL += rpl
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

	acctID := s.accountID
	if acctID == "" {
		acctID = os.Getenv("ACCOUNT_ID")
		if acctID == "" {
			acctID = "signalstack-challenger-001"
		}
	}
	acctNum := s.accountNumber
	if acctNum == "" {
		acctNum = os.Getenv("ACCOUNT_NUMBER")
		if acctNum == "" {
			acctNum = "SS-CHALLENGER-MAIN"
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
		PortfolioValue: totalMarketVal,
		RealizedPL:     realizedPL,
		UnrealizedPL:   totalUnrealizedPL,
		DayPL:          totalDayPL,
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
				_, _ = tx.ExecContext(ctx, `
					UPDATE execution_journal
					SET fill_price = ?, status = 'filled'
					WHERE id = ?;
				`, o.FillPrice, execID)

				if execSigID != "" {
					if o.RealizedPL != nil {
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
				}
			}
		}

		// B. Fallback: match by symbol, action, and shares
		if !matched && sym != "" && act != "" && o.Shares > 0 {
			var tradeID int64
			var tradeSigID string
			err := tx.QueryRowContext(ctx, `
				SELECT id, signal_id FROM paper_trades
				WHERE symbol = ? AND action = ? AND ROUND(shares) = ROUND(?)
				ORDER BY id DESC LIMIT 1;
			`, sym, act, o.Shares).Scan(&tradeID, &tradeSigID)
			if err == nil && tradeID > 0 {
				matched = true
				if o.RealizedPL != nil {
					_, _ = tx.ExecContext(ctx, `
						UPDATE paper_trades
						SET price = ?, realized_pl = ?
						WHERE id = ?;
					`, o.FillPrice, *o.RealizedPL, tradeID)
				} else {
					_, _ = tx.ExecContext(ctx, `
						UPDATE paper_trades
						SET price = ?
						WHERE id = ?;
					`, o.FillPrice, tradeID)
				}

				if tradeSigID != "" {
					_, _ = tx.ExecContext(ctx, `
						UPDATE execution_journal
						SET fill_price = ?, status = 'filled'
						WHERE signal_id = ?;
					`, o.FillPrice, tradeSigID)
				} else {
					_, _ = tx.ExecContext(ctx, `
						UPDATE execution_journal
						SET fill_price = ?, status = 'filled'
						WHERE id = (
							SELECT id FROM execution_journal
							WHERE symbol = ? AND action = ? AND ROUND(requested_shares) = ROUND(?)
							ORDER BY id DESC LIMIT 1
						);
					`, o.FillPrice, sym, act, o.Shares)
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

	// 3. Recompute realized_pl for all SELL trades in paper_trades based on actual BUY fill prices
	sRows, err := tx.QueryContext(ctx, `SELECT id, symbol, shares, price FROM paper_trades WHERE action = 'SELL' ORDER BY id ASC;`)
	if err == nil {
		type sellRec struct {
			id     int64
			symbol string
			shares float64
			price  float64
		}
		var sells []sellRec
		for sRows.Next() {
			var r sellRec
			if err := sRows.Scan(&r.id, &r.symbol, &r.shares, &r.price); err == nil {
				sells = append(sells, r)
			}
		}
		sRows.Close()

		for _, sr := range sells {
			var buyPrice float64
			err := tx.QueryRowContext(ctx, `
				SELECT price FROM paper_trades
				WHERE symbol = ? AND action = 'BUY' AND id < ?
				ORDER BY id DESC LIMIT 1;
			`, sr.symbol, sr.id).Scan(&buyPrice)
			if err == nil && buyPrice > 0 {
				rpl := sr.shares * (sr.price - buyPrice)
				_, _ = tx.ExecContext(ctx, `UPDATE paper_trades SET realized_pl = ? WHERE id = ?;`, rpl, sr.id)
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
