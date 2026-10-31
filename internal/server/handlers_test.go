package server

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rollingw/signalstack-agent/internal/broker"
	"github.com/rollingw/signalstack-agent/internal/config"
	"github.com/rollingw/signalstack-agent/internal/journal"
)

func TestBuildClosedTradeLedgerAndDashboardHTML(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_agent.db")
	sqliteJ, err := journal.NewSQLiteJournal(dbPath, 100000.0, 2.0, "2026-09-28")
	if err != nil {
		t.Fatalf("failed to create sqlite journal: %v", err)
	}
	defer sqliteJ.Close()

	multiJ := journal.NewMultiJournal(sqliteJ, nil, nil)
	cfg := &config.Config{
		Port:          "8080",
		AccountNumber: "PA3HW1A38DR2",
		ServiceName:   "SignalStack Challenger Trading Agent",
	}
	ssBroker := broker.NewSignalStackBroker("", "", "close", true, sqliteJ)
	srv := NewServer(cfg, ssBroker, multiJ)

	tBuy := time.Date(2026, 10, 28, 13, 39, 45, 0, time.UTC)
	tSell := time.Date(2026, 10, 28, 17, 23, 40, 0, time.UTC)
	trades := []*journal.PaperTrade{
		{
			ID:         1,
			Symbol:     "SPXL",
			Action:     "BUY",
			Shares:     8,
			Price:      287.2763,
			RealizedPL: 0,
			SignalID:   "sig-buy-SPXL-202610281200-8",
			Timestamp:  tBuy,
		},
		{
			ID:         2,
			Symbol:     "SPXL",
			Action:     "SELL",
			Shares:     8,
			Price:      287.5475,
			RealizedPL: 2.17,
			SignalID:   "sig-exit-ch-SPXL-202610281100-8",
			Timestamp:  tSell,
		},
		// Canceled after-hours trade should be filtered out
		{
			ID:         3,
			Symbol:     "TZA",
			Action:     "SELL",
			Shares:     55,
			Price:      44.40,
			RealizedPL: -31.93,
			SignalID:   "sig-exit-ch-TZA-202610291100-55",
			Timestamp:  tSell.Add(24 * time.Hour),
		},
	}

	neonMeta := &journal.NeonTradeMetadata{
		ZZTrades: []*journal.NeonZZTrade{
			{
				ID:         "zz-1",
				Symbol:     "SPXL",
				State:      "open",
				BuySignal:  "REPAINT_FLIP of SPXS (2026-10-28 12:00)",
				EntryDate:  time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC),
				EntryPrice: 287.28,
				Shares:     8,
				Cost:       2298.24,
			},
			{
				ID:         "zz-2",
				Symbol:     "LABU",
				State:      "closed",
				BuySignal:  "REPAINT_FLIP of LABD (2026-10-28 12:00)",
				EntryDate:  time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC),
				EntryPrice: 244.80,
				Shares:     10,
				Cost:       2448.00,
				ExitDate:   time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC),
				ExitPrice:  264.00,
				ExitReason: "OPP_LOW_LL of LABD (2026-10-29 12:00)",
				PL:         192.00,
				PLPct:      7.84,
				BarsHeld:   3,
			},
		},
		ChallengerExits: []*journal.NeonChallengerExit{
			{
				ID:         11,
				Symbol:     "SPXL",
				ExitDate:   "2026-10-28 11:00",
				SignalID:   "sig-exit-ch-SPXL-202610281100-8",
				ExitReason: "OPP_LOW_HL of SPXS (2026-10-28 11:00) [Intraday T+0 Exit]",
			},
		},
	}

	rows, weeks := buildClosedTradeLedger(trades, neonMeta)
	if len(rows) != 2 {
		t.Fatalf("expected 2 closed ledger rows (excluding canceled signal), got %d", len(rows))
	}
	if len(weeks) != 1 || weeks[0] != "2026-10-26" {
		t.Fatalf("expected week 2026-10-26, got %v", weeks)
	}

	acct := &broker.Account{
		AccountNumber: "PA3HW1A38DR2",
		Status:        "ACTIVE",
		Cash:          90267.50,
		BuyingPower:   180535.00,
		Equity:        101136.71,
		RealizedPL:    1360.54,
	}
	htmlOut := srv.renderDashboardHTML(acct, nil, nil, trades, neonMeta)

	requiredSnippets := []string{
		"Challenger M7d Historical &amp; Live Trade Ledger",
		`id="ledger-sym-select"`,
		`id="ledger-week-select"`,
		"Entry Date (CST)",
		"Broker Entry Fill",
		"Exit Date (CST)",
		"Broker Exit Fill",
		"Broker Cost Basis",
		"Realized P/L ($)",
		"Return %",
		"Enter Reason (CST)",
		"Exit Reason (CST)",
		"Held (Bars)",
		"OPP_LOW_HL of SPXS (2026-10-28 11:00) [Intraday T+0 Exit]",
		"REPAINT_FLIP of SPXS (2026-10-28 11:00)",
	}
	for _, snip := range requiredSnippets {
		if !strings.Contains(htmlOut, snip) {
			t.Errorf("dashboard HTML missing expected snippet %q", snip)
		}
	}
	if strings.Contains(htmlOut, `label="State"`) || strings.Contains(htmlOut, `>State<`) {
		t.Errorf("dashboard HTML should not contain open/closed State dropdown or column")
	}
}
