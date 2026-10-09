package journal

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rollingw/signalstack-agent/internal/broker"
)

func TestDayPLCalculations(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test_paper.db")

	j, err := NewSQLiteJournal(dbPath, 100000.0, 2.0, "2026-09-28")
	if err != nil {
		t.Fatalf("failed to create sqlite journal: %v", err)
	}
	defer j.Close()

	// 1. Position opened before today in the previous trading session (e.g. Friday before close)
	prevSession := getPrevTradingCloseCST().Add(-2 * time.Hour)
	_, err = j.db.Exec(`
		INSERT INTO paper_positions (symbol, shares, avg_entry_price, cost_basis, current_price, prev_close, unrealized_pl, opened_at, updated_at)
		VALUES ('SPXL', 8, 283.37, 2266.96, 288.04, 283.37, 37.36, ?, ?);
	`, prevSession, prevSession)
	if err != nil {
		t.Fatalf("failed to insert yesterday position: %v", err)
	}

	// 2. Sell SPXL today -> realizes +$37.36
	rpl, err := j.ApplySell(ctx, "SPXL", 8, 288.04, "sig-exit-spxl")
	if err != nil {
		t.Fatalf("failed to apply sell: %v", err)
	}
	if rpl < 37.35 || rpl > 37.37 {
		t.Errorf("expected realized PL ~37.36, got %f", rpl)
	}

	// 3a. Position opened TODAY: TECS 50 sh @ $48.99
	err = j.ApplyBuy(ctx, "TECS", 50, 48.99, "sig-buy-tecs")
	if err != nil {
		t.Fatalf("failed to apply buy: %v", err)
	}

	// Update mark price of TECS to 48.81 with prev_close 50.23 (from yesterday's market close)
	err = j.UpdateMarketPrice(ctx, "TECS", 48.81, 50.23)
	if err != nil {
		t.Fatalf("failed to update mark price: %v", err)
	}

	// 3b. Position opened over the WEEKEND (Saturday, after Friday close): LABU 10 sh @ $253.14
	weekend := getPrevTradingCloseCST().Add(24 * time.Hour) // Saturday
	_, err = j.db.Exec(`
		INSERT INTO paper_positions (symbol, shares, avg_entry_price, cost_basis, current_price, prev_close, unrealized_pl, opened_at, updated_at)
		VALUES ('LABU', 10, 253.14, 2531.40, 258.14, 248.72, 50.0, ?, ?);
	`, weekend, weekend)
	if err != nil {
		t.Fatalf("failed to insert weekend position: %v", err)
	}

	// 4. Verify GetPosition for TECS (opened today -> baseline is entry $48.99, not prev_close $50.23)
	pos, err := j.GetPosition(ctx, "TECS")
	if err != nil {
		t.Fatalf("failed to get position: %v", err)
	}
	expectedDayChange := 48.81 - 48.99
	if pos.DayChange < expectedDayChange-0.001 || pos.DayChange > expectedDayChange+0.001 {
		t.Errorf("expected DayChange %f, got %f", expectedDayChange, pos.DayChange)
	}
	expectedDayPL := 50.0 * (48.81 - 48.99) // -9.00
	if pos.DayPL < expectedDayPL-0.001 || pos.DayPL > expectedDayPL+0.001 {
		t.Errorf("expected DayPL %f, got %f", expectedDayPL, pos.DayPL)
	}

	// 5. Verify GetPosition for LABU (opened on weekend -> baseline must be entry $253.14, NOT Friday close $248.72)
	posLABU, err := j.GetPosition(ctx, "LABU")
	if err != nil {
		t.Fatalf("failed to get position for LABU: %v", err)
	}
	expectedLABUDayChange := 258.14 - 253.14 // +5.00
	if posLABU.DayChange < expectedLABUDayChange-0.001 || posLABU.DayChange > expectedLABUDayChange+0.001 {
		t.Errorf("expected LABU DayChange %f, got %f (must use avgEntry, not Friday prevClose)", expectedLABUDayChange, posLABU.DayChange)
	}
	expectedLABUDayPL := 10.0 * (258.14 - 253.14) // +50.00
	if posLABU.DayPL < expectedLABUDayPL-0.001 || posLABU.DayPL > expectedLABUDayPL+0.001 {
		t.Errorf("expected LABU DayPL %f, got %f", expectedLABUDayPL, posLABU.DayPL)
	}

	// 6. Verify GetAccount
	acct, err := j.GetAccount(ctx)
	if err != nil {
		t.Fatalf("failed to get account: %v", err)
	}
	// Total Day PL: Realized today (+37.36) + TECS Day PL (-9.00) + LABU Day PL (+50.00) = +78.36
	expectedTotalDayPL := 37.36 + (-9.00) + 50.00
	if acct.DayPL < expectedTotalDayPL-0.01 || acct.DayPL > expectedTotalDayPL+0.01 {
		t.Errorf("expected Account DayPL %f, got %f", expectedTotalDayPL, acct.DayPL)
	}
}

func TestReconcilePositions(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test_reconcile.db")

	j, err := NewSQLiteJournal(dbPath, 100000.0, 2.0, "2026-09-28")
	if err != nil {
		t.Fatalf("failed to create sqlite journal: %v", err)
	}
	defer j.Close()

	// Initial buy
	_ = j.ApplyBuy(ctx, "OLD_SYM", 10, 100.0, "sig-1")

	// Reconcile with 4 positions and replace = true
	items := []*broker.ReconcilePositionItem{
		{Symbol: "DRIP", Shares: 62, AvgEntryPrice: 35.09, CurrentPrice: 35.13, PrevClose: 35.27, CostBasis: 2175.58},
		{Symbol: "LABU", Shares: 10, AvgEntryPrice: 253.20, CurrentPrice: 242.22, PrevClose: 248.72, CostBasis: 2532.00},
	}

	orders := []*broker.ReconcileOrderFillItem{
		{Symbol: "DRIP", Action: "BUY", Shares: 62, FillPrice: 35.09},
	}

	err = j.ReconcilePositions(ctx, items, orders, 90553.41, true)
	if err != nil {
		t.Fatalf("ReconcilePositions failed: %v", err)
	}

	// Verify OLD_SYM was removed because replace = true
	oldPos, err := j.GetPosition(ctx, "OLD_SYM")
	if err != nil {
		t.Fatalf("GetPosition error: %v", err)
	}
	if oldPos != nil {
		t.Errorf("expected OLD_SYM to be removed, but found: %+v", oldPos)
	}

	// Verify DRIP
	drip, err := j.GetPosition(ctx, "DRIP")
	if err != nil || drip == nil {
		t.Fatalf("failed to get DRIP: %v", err)
	}
	if drip.Qty != 62 || drip.AvgEntryPrice != 35.09 || drip.CurrentPrice != 35.13 {
		t.Errorf("DRIP mismatch: %+v", drip)
	}

	// Verify cash
	acct, err := j.GetAccount(ctx)
	if err != nil {
		t.Fatalf("GetAccount error: %v", err)
	}
	if acct.Cash != 90553.41 {
		t.Errorf("expected cash 90553.41, got %f", acct.Cash)
	}
}

func TestReconcileOrderFillsAndClosedTrades(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test_fills.db")

	j, err := NewSQLiteJournal(dbPath, 100000.0, 2.0, "2026-09-28")
	if err != nil {
		t.Fatalf("failed to create sqlite journal: %v", err)
	}
	defer j.Close()

	// 1. Initial paper trades with simulated signal prices
	_ = j.ApplyBuy(ctx, "SPXL", 8, 287.41, "sig-buy-SPXL-202609292000-8")
	_, _ = j.ApplySell(ctx, "SPXL", 8, 288.04, "sig-exit-SPXL-202610021200-8")

	// 2. Reconcile with real Alpaca broker fills:
	// BUY was filled at $283.13, SELL was filled at $288.26375
	orders := []*broker.ReconcileOrderFillItem{
		{
			Symbol:        "SPXL",
			Action:        "BUY",
			Shares:        8,
			FillPrice:     283.13,
			ClientOrderID: "sig-buy-SPXL-202609292000-8",
		},
		{
			Symbol:        "SPXL",
			Action:        "SELL",
			Shares:        8,
			FillPrice:     288.26375,
			ClientOrderID: "sig-exit-SPXL-202610021200-8",
		},
	}

	err = j.ReconcilePositions(ctx, nil, orders, 100041.07, false)
	if err != nil {
		t.Fatalf("ReconcilePositions failed: %v", err)
	}

	// 3. Verify trades in paper_trades
	trades, err := j.GetRecentTrades(ctx, 10)
	if err != nil {
		t.Fatalf("GetRecentTrades failed: %v", err)
	}
	if len(trades) < 2 {
		t.Fatalf("expected at least 2 trades, got %d", len(trades))
	}

	var sellTrade, buyTrade *PaperTrade
	for _, tr := range trades {
		if tr.Symbol == "SPXL" && tr.Action == "SELL" {
			sellTrade = tr
		}
		if tr.Symbol == "SPXL" && tr.Action == "BUY" {
			buyTrade = tr
		}
	}

	if buyTrade == nil || buyTrade.Price != 283.13 {
		t.Errorf("expected buy trade price 283.13, got %+v", buyTrade)
	}

	expectedRPL := 8.0 * (288.26375 - 283.13) // +41.07
	if sellTrade == nil || sellTrade.Price != 288.26375 || sellTrade.RealizedPL < expectedRPL-0.01 || sellTrade.RealizedPL > expectedRPL+0.01 {
		t.Errorf("expected sell trade price 288.26375 and realized PL ~41.07, got %+v", sellTrade)
	}

	// 4. Verify account accounting
	acct, err := j.GetAccount(ctx)
	if err != nil {
		t.Fatalf("GetAccount failed: %v", err)
	}
	if acct.RealizedPL < 41.06 || acct.RealizedPL > 41.08 {
		t.Errorf("expected account RealizedPL ~41.07, got %f", acct.RealizedPL)
	}
	if acct.Equity < 100041.06 || acct.Equity > 100041.08 {
		t.Errorf("expected account Equity ~100041.07, got %f", acct.Equity)
	}
}
