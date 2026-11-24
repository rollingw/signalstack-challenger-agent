package server

import (
	"net/http"
	"net/http/httptest"
	"net/url"
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
		// Genuine canceled after-hours trade should be filtered out
		{
			ID:         3,
			Symbol:     "TNA",
			Action:     "SELL",
			Shares:     41,
			Price:      59.664,
			RealizedPL: 51.41,
			SignalID:   "sig-exit-ch-TNA-202610281500-41",
			Timestamp:  tSell.Add(3 * time.Hour),
		},
		// Filled Oct 29 TZA exit must NOT be filtered out
		{
			ID:         4,
			Symbol:     "TZA",
			Action:     "BUY",
			Shares:     55,
			Price:      44.9805,
			RealizedPL: 0,
			SignalID:   "sig-buy-TZA-202610291200-55",
			Timestamp:  tBuy.Add(24 * time.Hour),
		},
		{
			ID:         5,
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
	if len(rows) != 3 {
		t.Fatalf("expected 3 closed ledger rows (excluding canceled Oct 28 TNA, including filled Oct 29 TZA), got %d", len(rows))
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

func TestDashboardLoginGateAndSession(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_auth.db")
	sqliteJ, err := journal.NewSQLiteJournal(dbPath, 100000.0, 2.0, "2026-09-28")
	if err != nil {
		t.Fatalf("failed to create sqlite journal: %v", err)
	}
	defer sqliteJ.Close()

	multiJ := journal.NewMultiJournal(sqliteJ, nil, nil)
	cfg := &config.Config{
		Port:              "8080",
		AccountNumber:     "PA3HW1A38DR2",
		ServiceName:       "SignalStack Challenger Trading Agent",
		SignalSecret:      "test-secret",
		DashboardUsername: "rollingw",
		DashboardPassword: "arcamax1",
	}
	ssBroker := broker.NewSignalStackBroker("", "", "close", true, sqliteJ)
	srv := NewServer(cfg, ssBroker, multiJ)

	// 1. Unauthenticated browser request to GET / redirects to /login
	reqRoot := httptest.NewRequest(http.MethodGet, "/", nil)
	reqRoot.Header.Set("Accept", "text/html,application/xhtml+xml")
	rrRoot := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rrRoot, reqRoot)
	if rrRoot.Code != http.StatusSeeOther || rrRoot.Header().Get("Location") != "/login" {
		t.Fatalf("expected 303 redirect to /login, got %d (%s)", rrRoot.Code, rrRoot.Header().Get("Location"))
	}

	// 2. Invalid credentials redirect to /login?error=1
	badForm := url.Values{"username": {"rollingw"}, "password": {"wrongpw"}}
	reqBad := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(badForm.Encode()))
	reqBad.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rrBad := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rrBad, reqBad)
	if rrBad.Code != http.StatusSeeOther || rrBad.Header().Get("Location") != "/login?error=1" {
		t.Fatalf("expected 303 redirect to /login?error=1, got %d (%s)", rrBad.Code, rrBad.Header().Get("Location"))
	}

	// 3. Valid credentials (rollingw / arcamax1) set session cookie and redirect to /
	goodForm := url.Values{"username": {"rollingw"}, "password": {"arcamax1"}}
	reqGood := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(goodForm.Encode()))
	reqGood.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rrGood := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rrGood, reqGood)
	if rrGood.Code != http.StatusSeeOther || rrGood.Header().Get("Location") != "/" {
		t.Fatalf("expected 303 redirect to /, got %d (%s)", rrGood.Code, rrGood.Header().Get("Location"))
	}
	cookies := rrGood.Result().Cookies()
	var sessCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == dashboardSessionCookie {
			sessCookie = c
			break
		}
	}
	if sessCookie == nil || sessCookie.Value == "" {
		t.Fatalf("expected session cookie %q to be set", dashboardSessionCookie)
	}

	// 4. Authenticated browser request to GET / returns 200 dashboard HTML
	reqAuthed := httptest.NewRequest(http.MethodGet, "/", nil)
	reqAuthed.Header.Set("Accept", "text/html")
	reqAuthed.AddCookie(sessCookie)
	rrAuthed := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rrAuthed, reqAuthed)
	if rrAuthed.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for authenticated GET /, got %d", rrAuthed.Code)
	}
	if !strings.Contains(rrAuthed.Body.String(), "SignalStack Challenger Trading Agent") || !strings.Contains(rrAuthed.Body.String(), "Logout") {
		t.Fatalf("authenticated dashboard HTML missing expected content")
	}

	// 5. Logout clears session cookie and redirects to /login
	reqLogout := httptest.NewRequest(http.MethodGet, "/logout", nil)
	rrLogout := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rrLogout, reqLogout)
	if rrLogout.Code != http.StatusSeeOther || rrLogout.Header().Get("Location") != "/login" {
		t.Fatalf("expected 303 redirect to /login on logout, got %d", rrLogout.Code)
	}
}

func TestZeroLinesAndRepaintsSyncAndDashboardRender(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_zl.db")
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
		SignalSecret:  "test-secret",
	}
	ssBroker := broker.NewSignalStackBroker("", "", "close", true, sqliteJ)
	srv := NewServer(cfg, ssBroker, multiJ)

	// 1. Sync zero-line + repaint via POST /zero-lines/sync
	payload := `{
		"replace_open": true,
		"zero_lines": [{
			"pair_key": "JNUG/JDST",
			"entry_signal_id": "sig-buy-JNUG-202611131100-21",
			"active_symbol": "JNUG",
			"partner_symbol": "JDST",
			"armed_date": "2026-11-12 11:00",
			"pivot_label": "HL",
			"active_zero_price": 107.36,
			"partner_zero_price": 30.09,
			"active_entry_price": 109.65,
			"partner_entry_price": 30.09,
			"active_live_price": 71.20,
			"partner_live_price": 43.52,
			"active_vs_zero_pct": -33.68,
			"partner_vs_zero_pct": 44.63,
			"active_linear_state": "RED",
			"partner_linear_state": "GREEN_HOLD",
			"flip_count": 0,
			"zero_line_status": "FLIP_READY",
			"state": "OPEN",
			"repaint_count": 2
		}],
		"repaints": [{
			"pair_key": "JNUG/JDST",
			"symbol": "JNUG",
			"partner_symbol": "JDST",
			"bar_date": "2026-11-16 12:00",
			"event_type": "PIVOT_REPAINT",
			"orig_pivot_price": 107.36,
			"new_repaint_price": 102.01,
			"active_live_price": 102.01,
			"partner_live_price": 31.85,
			"active_vs_zero_pct": -4.98,
			"partner_vs_zero_pct": 5.85,
			"partner_linear_state": "GREEN_HOLD",
			"action_taken": "FLIP_READY",
			"note": "Macro HL @ $107.36 superseded by $102.01"
		}]
	}`
	reqSync := httptest.NewRequest(http.MethodPost, "/zero-lines/sync", strings.NewReader(payload))
	reqSync.Header.Set("Authorization", "Bearer test-secret")
	reqSync.Header.Set("Content-Type", "application/json")
	rrSync := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rrSync, reqSync)
	if rrSync.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from POST /zero-lines/sync, got %d: %s", rrSync.Code, rrSync.Body.String())
	}

	// 2. Verify GET /zero-lines & GET /repaints return persisted records
	reqZL := httptest.NewRequest(http.MethodGet, "/zero-lines", nil)
	rrZL := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rrZL, reqZL)
	if rrZL.Code != http.StatusOK || !strings.Contains(rrZL.Body.String(), "JDST") || !strings.Contains(rrZL.Body.String(), "FLIP_READY") {
		t.Fatalf("GET /zero-lines unexpected response (%d): %s", rrZL.Code, rrZL.Body.String())
	}

	reqRP := httptest.NewRequest(http.MethodGet, "/repaints", nil)
	rrRP := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rrRP, reqRP)
	if rrRP.Code != http.StatusOK || !strings.Contains(rrRP.Body.String(), "PIVOT_REPAINT") {
		t.Fatalf("GET /repaints unexpected response (%d): %s", rrRP.Code, rrRP.Body.String())
	}

	// 3. Verify paired dashboard HTML renders Active JNUG + Entry Pivot Zero Line + Opposite JDST + Audit Table
	positions := []*broker.Position{
		{
			Symbol:          "JNUG",
			Qty:             21,
			AvgEntryPrice:   109.65,
			CurrentPrice:    71.20,
			MarketValue:     1495.20,
			UnrealizedPL:    -807.45,
			UnrealizedPLPct: -35.07,
			OpenedAt:        time.Date(2026, 11, 13, 15, 2, 0, 0, time.UTC),
			UpdatedAt:       time.Date(2026, 11, 20, 21, 0, 0, 0, time.UTC),
		},
	}
	htmlOut := srv.renderDashboardHTML(&broker.Account{AccountNumber: "PA3HW1A38DR2", Status: "ACTIVE"}, positions, nil, nil, nil)
	for _, want := range []string{
		"Opposite Pair Zero Line",
		"HL <strong>$107.36</strong>",
		"⚡ FLIP READY",
		"JDST",
		"$43.52",
		"LIN GREEN HOLD",
		"Entry Pivot Repaints &amp; Zero-Line Audit Log (1 Events)",
		"PIVOT_REPAINT",
	} {
		if !strings.Contains(htmlOut, want) {
			t.Fatalf("dashboard HTML missing expected Zero-Line UI element: %q", want)
		}
	}
	ledgerIdx := strings.Index(htmlOut, "Challenger M7d Historical &amp; Live Trade Ledger")
	repaintsIdx := strings.Index(htmlOut, "Entry Pivot Repaints &amp; Zero-Line Audit Log")
	if ledgerIdx < 0 || repaintsIdx < 0 || ledgerIdx >= repaintsIdx {
		t.Fatalf("expected Historical & Live Trade Ledger (idx=%d) to render above Entry Pivot Repaints & Zero-Line Audit Log (idx=%d)", ledgerIdx, repaintsIdx)
	}
}
