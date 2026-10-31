package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rollingw/signalstack-agent/internal/broker"
	"github.com/rollingw/signalstack-agent/internal/journal"
	"github.com/rollingw/signalstack-agent/internal/safety"
)

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	acct, _ := s.broker.GetAccount(ctx)
	positions, _ := s.broker.GetAllPositions(ctx)
	recent, _ := s.journal.GetRecentExecutions(ctx, 15)
	trades, _ := s.journal.SQLite().GetRecentTrades(ctx, 50)

	if strings.Contains(r.Header.Get("Accept"), "text/html") {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, s.renderDashboardHTML(acct, positions, recent, trades))
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"service":           s.cfg.ServiceName,
		"model":             "Model 5 (Linear Green Hold & Symmetric Dual-Hybrid)",
		"role":              "challenger",
		"status":            "running",
		"champion_url":      s.cfg.ChampionURL,
		"broker":            s.broker.Name(),
		"bootstrap_mode":    s.broker.BootstrapMode(),
		"account":           acct,
		"positions_count":   len(positions),
		"positions":         positions,
		"recent_executions": len(recent),
		"endpoints": map[string]string{
			"GET /":          "Service dashboard (HTML/JSON)",
			"GET /health":    "Health and liveness check",
			"GET /status":    "Account status, balances, and positions",
			"GET /positions": "List open paper positions",
			"GET /journal":   "Recent execution records",
			"GET /trades":    "Recent closed trade history with realized P/L",
			"POST /signal":   "Trade instruction endpoint (requires Bearer token)",
			"POST /price":    "Update symbol mark price (requires Bearer token)",
			"POST /positions/reconcile": "Reconcile positions and balances from broker (requires Bearer token)",
		},
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func formatCurrency(val float64) string {
	sign := ""
	if val < 0 {
		sign = "-"
		val = -val
	}
	intPart := int64(val)
	cents := int64(math.Round((val - float64(intPart)) * 100))
	if cents >= 100 {
		intPart++
		cents -= 100
	}
	s := strconv.FormatInt(intPart, 10)
	n := len(s)
	if n <= 3 {
		return fmt.Sprintf("%s$%s.%02d", sign, s, cents)
	}
	var b strings.Builder
	b.WriteString(sign)
	b.WriteByte('$')
	for i, ch := range s {
		if i > 0 && (n-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(ch)
	}
	fmt.Fprintf(&b, ".%02d", cents)
	return b.String()
}

var cstLocation = func() *time.Location {
	loc, err := time.LoadLocation("America/Chicago")
	if err != nil {
		return time.FixedZone("CST", -6*3600)
	}
	return loc
}()

func formatCST(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.In(cstLocation).Format("2006-01-02 15:04:05") + " CST"
}

func formatCSTShort(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.In(cstLocation).Format("2006-01-02 15:04") + " CST"
}

// isMarketHours checks if current time in America/Chicago is during an active trading day session
// (Monday through Friday, 8:15 AM - 3:30 PM CT, excluding major NYSE holidays).
func isMarketHours() bool {
	now := time.Now().In(cstLocation)
	weekday := now.Weekday()
	if weekday == time.Sunday || weekday == time.Saturday {
		return false
	}

	_, m, d := now.Date()
	if (m == time.January && d == 1) || // New Year's Day
		(m == time.June && d == 19) || // Juneteenth
		(m == time.July && d == 4) || // Independence Day
		(m == time.December && d == 25) { // Christmas
		return false
	}

	currMins := now.Hour()*60 + now.Minute()
	return currMins >= 495 && currMins <= 930
}

func (s *Server) renderDashboardHTML(
	acct *broker.Account,
	positions []*broker.Position,
	recent []*journal.ExecutionRecord,
	trades []*journal.PaperTrade,
) string {
	acctNum := s.cfg.AccountNumber
	if acctNum == "" {
		acctNum = "SS-CHALLENGER-MAIN"
	}
	acctStatus := "ACTIVE"
	var buyingPowerStr, cashStr, equityStr, plStr, dayPLStr string
	var realizedStr, unrealizedStr string
	realizedColor := "#94a3b8"
	realizedClass := ""
	unrealizedColor := "#94a3b8"
	dayPLClass := ""
	plClass := ""
	if acct != nil {
		acctNum = acct.AccountNumber
		acctStatus = strings.ToUpper(acct.Status)
		buyingPowerStr = formatCurrency(acct.BuyingPower)
		cashStr = formatCurrency(acct.Cash)
		equityStr = formatCurrency(acct.Equity)
		totalPL := acct.RealizedPL + acct.UnrealizedPL
		if totalPL > 0 {
			plStr = "+" + formatCurrency(totalPL)
			plClass = "card-value-green"
		} else if totalPL < 0 {
			plStr = formatCurrency(totalPL)
			plClass = "card-value-red"
		} else {
			plStr = "$0.00"
			plClass = ""
		}

		realizedStr = formatCurrency(acct.RealizedPL)
		if acct.RealizedPL > 0 {
			realizedStr = "+" + formatCurrency(acct.RealizedPL)
			realizedColor = "#34d399"
			realizedClass = "card-value-green"
		} else if acct.RealizedPL < 0 {
			realizedColor = "#f87171"
			realizedClass = "card-value-red"
		}

		unrealizedStr = formatCurrency(acct.UnrealizedPL)
		if acct.UnrealizedPL > 0 {
			unrealizedStr = "+" + formatCurrency(acct.UnrealizedPL)
			unrealizedColor = "#34d399"
		} else if acct.UnrealizedPL < 0 {
			unrealizedColor = "#f87171"
		}

		if acct.DayPL > 0 {
			dayPLStr = "+" + formatCurrency(acct.DayPL)
			dayPLClass = "card-value-green"
		} else if acct.DayPL < 0 {
			dayPLStr = formatCurrency(acct.DayPL)
			dayPLClass = "card-value-red"
		} else {
			dayPLStr = "$0.00"
			dayPLClass = ""
		}
	} else {
		buyingPowerStr = "$0.00"
		cashStr = "$0.00"
		equityStr = "$0.00"
		plStr = "$0.00"
		realizedStr = "$0.00"
		unrealizedStr = "$0.00"
		dayPLStr = "$0.00"
	}

	closedWins := 0
	closedLosses := 0
	for _, t := range trades {
		if t.Action == "SELL" {
			if t.RealizedPL > 0 {
				closedWins++
			} else if t.RealizedPL < 0 {
				closedLosses++
			}
		}
	}
	totalClosedTrades := closedWins + closedLosses
	winRateStr := "100% Win Rate"
	if totalClosedTrades > 0 {
		winRate := (float64(closedWins) / float64(totalClosedTrades)) * 100.0
		winRateStr = fmt.Sprintf("%.0f%% Win Rate (%d of %d wins)", winRate, closedWins, totalClosedTrades)
	}

	var latestUpdate time.Time
	for _, p := range positions {
		if p.UpdatedAt.After(latestUpdate) {
			latestUpdate = p.UpdatedAt
		}
	}
	lastUpdatedStr := formatCST(time.Now())
	if !latestUpdate.IsZero() {
		lastUpdatedStr = formatCST(latestUpdate)
	}

	// Positions HTML
	posRowsHTML := ""
	if len(positions) == 0 {
		posRowsHTML = `<tr><td colspan="9" style="text-align: center; color: #94a3b8; padding: 22px; font-size: 13px;">No open positions (Flat cash — Ready for incoming bootstrap buy signals)</td></tr>`
	} else {
		for _, p := range positions {
			plColor := "#10b981"
			if p.UnrealizedPL < 0 {
				plColor = "#f87171"
			}
			dayColor := "#94a3b8"
			dayStr := "-"
			if p.DayChange > 0 {
				dayColor = "#10b981"
				dayStr = fmt.Sprintf("+%s (+%.2f%%)", formatCurrency(p.DayChange), p.DayChangePct)
			} else if p.DayChange < 0 {
				dayColor = "#f87171"
				dayStr = fmt.Sprintf("%s (%.2f%%)", formatCurrency(p.DayChange), p.DayChangePct)
			} else if p.PrevClosePrice > 0 || p.AvgEntryPrice > 0 {
				dayStr = "$0.00 (0.00%)"
			}
			posRowsHTML += fmt.Sprintf(`
			<tr style="border-bottom: 1px solid #1e293b;">
				<td style="padding: 10px 14px; font-weight: 700; color: #ffffff; font-size: 14px;">%s</td>
				<td style="padding: 10px 14px; text-align: right; color: #ffffff; font-weight: 600;">%g</td>
				<td style="padding: 10px 14px; text-align: right; color: #38bdf8; font-weight: 700;">$%.2f</td>
				<td style="padding: 10px 14px; text-align: right; color: #ffffff; font-weight: 600;">$%.2f</td>
				<td style="padding: 10px 14px; text-align: right; font-weight: 700; color: %s;">%s</td>
				<td style="padding: 10px 14px; text-align: right; color: #ffffff; font-weight: 600;">%s</td>
				<td style="padding: 10px 14px; text-align: right; font-weight: 700; color: %s;">%s (%+.2f%%)</td>
				<td style="padding: 10px 14px; font-family: monospace; font-size: 12px; color: #38bdf8; font-weight: 600;">%s</td>
				<td style="padding: 10px 14px; font-family: monospace; font-size: 12px; color: #94a3b8;">%s</td>
			</tr>`,
				p.Symbol, p.Qty, p.AvgEntryPrice, p.CurrentPrice,
				dayColor, dayStr,
				formatCurrency(p.MarketValue), plColor, formatCurrency(p.UnrealizedPL), p.UnrealizedPLPct,
				formatCST(p.UpdatedAt),
				formatCSTShort(p.OpenedAt),
			)
		}
	}

	// Executions HTML
	execRowsHTML := ""
	if len(recent) == 0 {
		execRowsHTML = `<tr><td colspan="8" style="text-align: center; color: #94a3b8; padding: 22px; font-size: 13px;">No executions recorded yet</td></tr>`
	} else {
		for _, r := range recent {
			statusColor := "#10b981"
			statusBg := "#064e3b"
			st := strings.ToLower(r.Status)
			if strings.Contains(st, "rejected") || strings.Contains(st, "failed") {
				statusColor = "#f87171"
				statusBg = "#7f1d1d"
			} else if strings.Contains(st, "skip") {
				statusColor = "#fbbf24"
				statusBg = "#78350f"
			} else if strings.Contains(st, "accepted") {
				statusColor = "#38bdf8"
				statusBg = "#0c4a6e"
			}
			actionColor := "#34d399"
			if r.Action == "SELL" {
				actionColor = "#fb7185"
			}

			fillPriceStr := "-"
			if r.FillPrice > 0 {
				fillPriceStr = fmt.Sprintf("$%.2f", r.FillPrice)
			}

			dispShares := r.RequestedShares
			if r.FilledShares > 0 {
				dispShares = r.FilledShares
			}

			execRowsHTML += fmt.Sprintf(`
			<tr style="border-bottom: 1px solid #1e293b;">
				<td style="padding: 10px 14px; font-family: monospace; font-size: 12px; color: #94a3b8;">%s</td>
				<td style="padding: 10px 14px; font-weight: 700; color: #ffffff; font-size: 14px;">%s</td>
				<td style="padding: 10px 14px; font-weight: 700; color: %s; font-size: 13px;">%s</td>
				<td style="padding: 10px 14px; text-align: right; color: #ffffff; font-weight: 600;">%g</td>
				<td style="padding: 10px 14px; text-align: right; color: #cbd5e1; font-weight: 600;">$%.2f</td>
				<td style="padding: 10px 14px; text-align: right; color: #34d399; font-weight: 700;">%s</td>
				<td style="padding: 10px 14px; text-align: center;"><span style="background: %s; color: %s; padding: 3px 8px; border-radius: 4px; font-size: 11px; font-weight: 700;">%s</span></td>
				<td style="padding: 10px 14px; font-family: monospace; font-size: 12px; color: #94a3b8;">%s</td>
			</tr>`,
				r.SignalID, r.Symbol,
				actionColor, r.Action,
				dispShares, r.SignalPrice,
				fillPriceStr,
				statusBg, statusColor, strings.ToUpper(r.Status),
				formatCST(r.SubmittedAt),
			)
		}
	}

	// Closed Trades HTML
	closedRowsHTML := ""
	closedCount := 0
	for _, t := range trades {
		if t.Action != "SELL" {
			continue
		}
		closedCount++
		plColor := "#94a3b8"
		plDisplay := "$0.00"
		if t.RealizedPL > 0 {
			plColor = "#10b981"
			plDisplay = "+" + formatCurrency(t.RealizedPL)
		} else if t.RealizedPL < 0 {
			plColor = "#f87171"
			plDisplay = formatCurrency(t.RealizedPL)
		}

		sigID := t.SignalID
		if sigID == "" {
			sigID = "-"
		}

		closedRowsHTML += fmt.Sprintf(`
		<tr style="border-bottom: 1px solid #1e293b;">
			<td style="padding: 10px 14px; font-weight: 700; color: #ffffff; font-size: 14px;">%s</td>
			<td style="padding: 10px 14px; font-weight: 700; color: #fb7185; font-size: 13px;">SELL</td>
			<td style="padding: 10px 14px; text-align: right; color: #ffffff; font-weight: 600;">%g</td>
			<td style="padding: 10px 14px; text-align: right; color: #34d399; font-weight: 700;">$%.2f</td>
			<td style="padding: 10px 14px; text-align: right; font-weight: 700; color: %s; font-size: 14px;">%s</td>
			<td style="padding: 10px 14px; font-family: monospace; font-size: 12px; color: #94a3b8;">%s</td>
			<td style="padding: 10px 14px; font-family: monospace; font-size: 12px; color: #38bdf8;">%s</td>
		</tr>`,
			t.Symbol,
			t.Shares,
			t.Price,
			plColor, plDisplay,
			sigID,
			formatCST(t.Timestamp),
		)
	}
	if closedCount == 0 {
		closedRowsHTML = `<tr><td colspan="7" style="text-align: center; color: #94a3b8; padding: 22px; font-size: 13px;">No closed trades recorded yet</td></tr>`
	}

	webhookStatus := "Configured"
	webhookColor := "#10b981"
	if s.broker.HasAlpaca() {
		webhookStatus = "Live Alpaca Paper API Connected (PA3HW1A38DR2)"
		webhookColor = "#10b981"
	} else if s.broker.WebhookURL() == "" {
		webhookStatus = "Simulation Mode (Set SIGNALSTACK_WEBHOOK_URL)"
		webhookColor = "#fbbf24"
	}

	refreshMetaTag := ""
	refreshStatusHTML := `<span style="color: #94a3b8; font-size: 11px; font-weight: 600;">⏸️ Market Closed (Auto-refresh paused)</span>`
	if isMarketHours() {
		refreshMetaTag = `<meta http-equiv="refresh" content="30">`
		refreshStatusHTML = `<span style="color: #10b981; font-size: 11px; font-weight: 600;">🔄 Market Open (Auto-refreshing 30s)</span>`
	}

	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	%s
	<title>SignalStack Challenger Trading Agent Dashboard</title>
	<style>
		* { box-sizing: border-box; }
		body {
			font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
			background-color: #0b0f19;
			color: #f1f5f9;
			margin: 0;
			padding: 24px 20px;
			line-height: 1.5;
		}
		.container { max-width: 1200px; margin: 0 auto; }
		.header {
			display: flex;
			justify-content: space-between;
			align-items: center;
			margin-bottom: 24px;
			padding-bottom: 18px;
			border-bottom: 1px solid #1e293b;
			flex-wrap: wrap;
			gap: 12px;
		}
		.app-title { font-size: 24px; font-weight: 800; margin: 0; display: flex; align-items: center; gap: 8px; color: #e0e7ff; }
		.app-subtitle { font-size: 13px; color: #94a3b8; margin: 4px 0 0 0; }
		.pill {
			display: inline-block;
			padding: 3px 10px;
			border-radius: 9999px;
			font-size: 12px;
			font-weight: 600;
			background: #1e293b;
			color: #a5b4fc;
		}
		.bootstrap-badge {
			display: inline-flex;
			align-items: center;
			gap: 6px;
			background: rgba(16, 185, 129, 0.15);
			border: 1px solid rgba(16, 185, 129, 0.4);
			color: #34d399;
			padding: 4px 10px;
			border-radius: 9999px;
			font-size: 11px;
			font-weight: 700;
		}
		.cards {
			display: grid;
			grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
			gap: 14px;
			margin-bottom: 28px;
		}
		.card {
			background: #111827;
			border: 1px solid #1f2937;
			border-radius: 10px;
			padding: 16px 20px;
		}
		.card-title { font-size: 12px; text-transform: uppercase; color: #9ca3af; font-weight: 700; letter-spacing: 0.05em; }
		.card-value { font-size: 24px; font-weight: 800; color: #ffffff; margin-top: 6px; }
		.card-value-green { color: #34d399; }
		.card-value-blue { color: #818cf8; }
		.card-value-red { color: #f87171; }
		.panel {
			background: #111827;
			border: 1px solid #1f2937;
			border-radius: 10px;
			padding: 20px;
			margin-bottom: 24px;
		}
		.panel-title { font-size: 16px; font-weight: 700; margin: 0 0 16px 0; color: #f3f4f6; }
		table { width: 100%%; border-collapse: collapse; text-align: left; }
		th { padding: 10px 14px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: #94a3b8; border-bottom: 1px solid #374151; font-weight: 700; }
		.endpoint-item {
			display: flex;
			justify-content: space-between;
			padding: 10px 0;
			border-bottom: 1px solid #1f2937;
			font-size: 13px;
		}
		.endpoint-method {
			background: #4f46e5;
			color: #ffffff;
			font-family: monospace;
			padding: 2px 6px;
			border-radius: 4px;
			font-weight: 700;
			margin-right: 8px;
		}
	</style>
</head>
<body>
	<div class="container">
		<div class="header">
			<div>
				<h1 class="app-title">⚡ SignalStack Challenger Trading Agent</h1>
				<p class="app-subtitle">Model 5 (Linear Green Hold & Symmetric Dual-Hybrid) Shadow Gateway &bull; Broker: <span class="pill">%s</span> &bull; Webhook: <span style="color: %s; font-weight: 600;">%s</span> &bull; <span style="color: #a5b4fc; font-weight: 700;">Last Updated: %s</span> &bull; %s</p>
			</div>
			<div style="display: flex; flex-direction: column; align-items: flex-end; gap: 6px;">
				<span style="display: inline-flex; align-items: center; gap: 6px; background: rgba(99, 102, 241, 0.2); border: 1px solid rgba(129, 140, 248, 0.5); color: #c7d2fe; padding: 6px 14px; border-radius: 9999px; font-size: 12px; font-weight: 700;">🏆 CHALLENGER MODEL 5 &bull; LINEAR GREEN HOLD &amp; SYMMETRIC DUAL-HYBRID</span>
				<div style="display: flex; align-items: center; gap: 8px;">
					<span class="bootstrap-badge">🚀 BOOTSTRAP ACTIVE</span>
					<a href="https://signalstack-trading-agent.fly.dev" target="_blank" style="text-decoration:none; color: #a5b4fc; background: #312e81; border: 1px solid #6366f1; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 700;">↗️ View Champion Agent</a>
				</div>
				<span style="font-size: 11px; color: #94a3b8; font-family: monospace;">Times: Central Time (CST/CDT)</span>
			</div>
		</div>

		<div class="cards">
			<div class="card">
				<div class="card-title">Account / Status</div>
				<div class="card-value" style="font-family: monospace; font-size: 19px;">%s <span style="font-size: 11px; padding: 2px 6px; border-radius: 4px; background: #064e3b; color: #34d399; vertical-align: middle;">%s</span></div>
			</div>
			<div class="card">
				<div class="card-title">Available Buying Power</div>
				<div class="card-value card-value-green">%s</div>
			</div>
			<div class="card">
				<div class="card-title">Cash Balance</div>
				<div class="card-value card-value-blue">%s</div>
			</div>
			<div class="card">
				<div class="card-title">Portfolio Equity</div>
				<div class="card-value">%s</div>
			</div>
			<div class="card">
				<div class="card-title">Today's Day P&amp;L</div>
				<div class="card-value %s">%s</div>
				<div style="font-size: 11px; color: #94a3b8; margin-top: 5px;">
					Today's closed fills + %d open holding(s) MTM
				</div>
			</div>
			<div class="card">
				<div class="card-title">Closed Trades Realized P&amp;L</div>
				<div class="card-value %s">%s</div>
				<div style="font-size: 11px; color: #94a3b8; margin-top: 5px;">
					<span style="color: %s; font-weight: 700;">🏆 %s</span>
				</div>
			</div>
			<div class="card">
				<div class="card-title">Total Net P&amp;L</div>
				<div class="card-value %s">%s</div>
				<div style="font-size: 11px; color: #94a3b8; margin-top: 5px;">
					Realized: <span style="color: %s; font-weight: 700;">%s</span> &bull; Unrealized: <span style="color: %s; font-weight: 700;">%s</span>
				</div>
			</div>
		</div>

		<div class="panel">
			<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; flex-wrap: wrap; gap: 8px;">
				<h2 class="panel-title" style="margin: 0;">Active Paper Positions (Holdings: %d)</h2>
				<span style="font-size: 12px; font-weight: 700; color: #38bdf8; background: #0c4a6e; border: 1px solid #0284c7; padding: 4px 10px; border-radius: 6px;">⏱️ Last Updated: %s</span>
			</div>
			<div style="overflow-x: auto;">
				<table>
					<thead>
						<tr>
							<th>Symbol</th>
							<th style="text-align: right;">Shares</th>
							<th style="text-align: right;">Live Broker Fill</th>
							<th style="text-align: right;">Mark Price</th>
							<th style="text-align: right;">Today's Change</th>
							<th style="text-align: right;">Market Value</th>
							<th style="text-align: right;">Total Unrealized P/L</th>
							<th>Last Updated (CST)</th>
							<th>Opened At (CST)</th>
						</tr>
					</thead>
					<tbody>
						%s
					</tbody>
				</table>
			</div>
		</div>

		<div class="panel">
			<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; flex-wrap: wrap; gap: 8px;">
				<h2 class="panel-title" style="margin: 0;">Recent Closed Trades (%d Closed Fills)</h2>
				<span style="font-size: 12px; font-weight: 600; color: #94a3b8;">Authoritative live broker exit fills</span>
			</div>
			<div style="overflow-x: auto;">
				<table>
					<thead>
						<tr>
							<th>Symbol</th>
							<th>Action</th>
							<th style="text-align: right;">Shares</th>
							<th style="text-align: right;">Live Broker Exit Fill</th>
							<th style="text-align: right;">Realized Profit / Loss</th>
							<th>Signal ID</th>
							<th>Closed At (CST)</th>
						</tr>
					</thead>
					<tbody>
						%s
					</tbody>
				</table>
			</div>
		</div>

		<div class="panel">
			<h2 class="panel-title">Recent Execution Journal</h2>
			<div style="overflow-x: auto;">
				<table>
					<thead>
						<tr>
							<th>Signal ID</th>
							<th>Symbol</th>
							<th>Action</th>
							<th style="text-align: right;">Shares</th>
							<th style="text-align: right;">Signal Price</th>
							<th style="text-align: right;">Live Broker Fill</th>
							<th style="text-align: center;">Status</th>
							<th>Submitted At (CST)</th>
						</tr>
					</thead>
					<tbody>
						%s
					</tbody>
				</table>
			</div>
		</div>

		<div class="panel">
			<h2 class="panel-title">Available Endpoints</h2>
			<div class="endpoint-item">
				<div><span class="endpoint-method">GET</span> /health</div>
				<div style="color: #94a3b8;">Liveness check and broker status</div>
			</div>
			<div class="endpoint-item">
				<div><span class="endpoint-method">GET</span> /status</div>
				<div style="color: #94a3b8;">Full paper account details, cash, buying power, and holdings</div>
			</div>
			<div class="endpoint-item">
				<div><span class="endpoint-method">GET</span> /positions</div>
				<div style="color: #94a3b8;">JSON list of all open positions in paper portfolio</div>
			</div>
			<div class="endpoint-item">
				<div><span class="endpoint-method">GET</span> /journal</div>
				<div style="color: #94a3b8;">Execution journal linking signals to orders</div>
			</div>
			<div class="endpoint-item">
				<div><span class="endpoint-method">GET</span> /trades</div>
				<div style="color: #94a3b8;">Closed trade log with realized profit/loss calculations</div>
			</div>
			<div class="endpoint-item">
				<div><span class="endpoint-method">POST</span> /signal</div>
				<div style="color: #94a3b8;">Broker-neutral trade instruction endpoint (Bearer token required)</div>
			</div>
			<div class="endpoint-item">
				<div><span class="endpoint-method">POST</span> /positions/reconcile</div>
				<div style="color: #94a3b8;">Reconcile positions and balances from broker (Bearer token required)</div>
			</div>
		</div>
	</div>
</body>
</html>`,
		refreshMetaTag,
		s.broker.Name(), webhookColor, webhookStatus, lastUpdatedStr, refreshStatusHTML,
		acctNum, acctStatus, buyingPowerStr, cashStr, equityStr,
		dayPLClass, dayPLStr, len(positions),
		realizedClass, realizedStr, realizedColor, winRateStr,
		plClass, plStr, realizedColor, realizedStr, unrealizedColor, unrealizedStr,
		len(positions), lastUpdatedStr, posRowsHTML,
		closedCount, closedRowsHTML,
		execRowsHTML,
	)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]any{
		"status":         "healthy",
		"broker":         s.broker.Name(),
		"bootstrap_mode": s.broker.BootstrapMode(),
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	acct, err := s.broker.GetAccount(ctx)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":   "account_error",
			"message": err.Error(),
		})
		return
	}

	positions, _ := s.broker.GetAllPositions(ctx)
	recent, _ := s.journal.GetRecentExecutions(ctx, 10)

	var latestUpdate time.Time
	for _, p := range positions {
		if p.UpdatedAt.After(latestUpdate) {
			latestUpdate = p.UpdatedAt
		}
	}
	lastUpdatedCST := formatCST(time.Now())
	if !latestUpdate.IsZero() {
		lastUpdatedCST = formatCST(latestUpdate)
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"agent":                   "running",
		"broker":                  s.broker.Name(),
		"bootstrap_mode":          s.broker.BootstrapMode(),
		"account":                 acct,
		"open_positions_count":    len(positions),
		"open_positions":          positions,
		"recent_executions_count": len(recent),
		"recent_executions":       recent,
		"timestamp":               time.Now().UTC().Format(time.RFC3339),
		"last_updated_cst":        lastUpdatedCST,
	})
}

func (s *Server) handlePositions(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	positions, err := s.broker.GetAllPositions(ctx)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":   "database_error",
			"message": err.Error(),
		})
		return
	}

	var latestUpdate time.Time
	type enrichedPosition struct {
		*broker.Position
		UpdatedAtCST string `json:"updated_at_cst"`
		OpenedAtCST  string `json:"opened_at_cst"`
	}
	enriched := make([]enrichedPosition, 0, len(positions))
	for _, p := range positions {
		if p.UpdatedAt.After(latestUpdate) {
			latestUpdate = p.UpdatedAt
		}
		enriched = append(enriched, enrichedPosition{
			Position:     p,
			UpdatedAtCST: formatCST(p.UpdatedAt),
			OpenedAtCST:  formatCST(p.OpenedAt),
		})
	}

	lastUpdatedCST := formatCST(time.Now())
	if !latestUpdate.IsZero() {
		lastUpdatedCST = formatCST(latestUpdate)
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"total":            len(positions),
		"positions":        enriched,
		"last_updated_cst": lastUpdatedCST,
	})
}

func (s *Server) handleJournal(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	records, err := s.journal.GetRecentExecutions(ctx, 50)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":   "database_error",
			"message": err.Error(),
		})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"total":   len(records),
		"records": records,
	})
}

func (s *Server) handleTrades(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	trades, err := s.journal.SQLite().GetRecentTrades(ctx, 50)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":   "database_error",
			"message": err.Error(),
		})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"total":  len(trades),
		"trades": trades,
	})
}

func (s *Server) handlePriceUpdate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Symbol    string  `json:"symbol"`
		Price     float64 `json:"price"`
		PrevClose float64 `json:"prev_close,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_json"})
		return
	}

	if req.Symbol == "" || req.Price <= 0 {
		s.writeJSON(w, http.StatusBadRequest, map[string]any{"error": "symbol and price > 0 required"})
		return
	}

	if err := s.broker.UpdateMarketPrice(r.Context(), req.Symbol, req.Price, req.PrevClose); err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"status":     "updated",
		"symbol":     strings.ToUpper(req.Symbol),
		"price":      req.Price,
		"prev_close": req.PrevClose,
	})
}

func (s *Server) handleReconcilePositions(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Positions []*broker.ReconcilePositionItem  `json:"positions"`
		Orders    []*broker.ReconcileOrderFillItem `json:"orders,omitempty"`
		Cash      float64                         `json:"cash,omitempty"`
		Replace   bool                            `json:"replace,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":   "invalid_json",
			"message": err.Error(),
		})
		return
	}

	if err := s.broker.ReconcilePositions(r.Context(), req.Positions, req.Orders, req.Cash, req.Replace); err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":   "reconcile_failed",
			"message": err.Error(),
		})
		return
	}

	positions, _ := s.broker.GetAllPositions(r.Context())
	acct, _ := s.broker.GetAccount(r.Context())

	s.writeJSON(w, http.StatusOK, map[string]any{
		"status":          "reconciled",
		"positions_count": len(positions),
		"positions":       positions,
		"account":         acct,
	})
}

func (s *Server) handleSignal(w http.ResponseWriter, r *http.Request) {
	var sig safety.TradeSignal
	if err := json.NewDecoder(r.Body).Decode(&sig); err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":   "invalid_json",
			"message": fmt.Sprintf("Failed to parse trade signal JSON: %v", err),
		})
		return
	}

	// 1. Validate instruction structure and values
	if err := safety.ValidateInstruction(&sig); err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":   "validation_failed",
			"message": err.Error(),
		})
		return
	}

	// 2. Reject duplicate signal_ids (Strict Idempotency Protection)
	isDup, existing, err := s.journal.IsDuplicate(r.Context(), sig.SignalID)
	if err != nil {
		log.Printf("[Agent Server] Error checking idempotency for %s: %v", sig.SignalID, err)
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":   "internal_error",
			"message": "Failed to verify signal idempotency",
		})
		return
	}
	if isDup {
		log.Printf("[Agent Server] Duplicate signal rejected: %s", sig.SignalID)
		s.writeJSON(w, http.StatusConflict, map[string]any{
			"error":           "duplicate_signal",
			"message":         fmt.Sprintf("Signal ID '%s' has already been processed", sig.SignalID),
			"signal_id":       sig.SignalID,
			"existing_record": existing,
		})
		return
	}

	// 3. Fetch broker account details for safety checks
	acct, err := s.broker.GetAccount(r.Context())
	if err != nil {
		log.Printf("[Agent Server] Failed to get broker account: %v", err)
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":   "account_error",
			"message": fmt.Sprintf("Could not retrieve paper account status: %v", err),
		})
		return
	}

	// 4. Enforce hard safety limits
	if err := safety.EnforceHardSafetyLimits(&sig, acct, s.cfg); err != nil {
		log.Printf("[Agent Server] Safety violation for %s: %v", sig.SignalID, err)
		rejectedRec := &journal.ExecutionRecord{
			SignalID:        sig.SignalID,
			Broker:          s.broker.Name(),
			ClientOrderID:   sig.SignalID,
			Symbol:          sig.Symbol,
			Action:          sig.Action,
			RequestedShares: sig.Shares,
			SignalPrice:     sig.SignalPrice,
			Status:          "rejected",
			Strategy:        sig.Strategy,
			SubmittedAt:     time.Now().UTC(),
			ErrorMessage:    err.Error(),
			CreatedAt:       time.Now().UTC(),
		}
		_ = s.journal.RecordExecution(r.Context(), rejectedRec)

		s.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"error":     "safety_limit_violation",
			"message":   err.Error(),
			"signal_id": sig.SignalID,
		})
		return
	}

	// 4b. Enforce Active Position & Inverse Pair Mutual Exclusion on BUY orders
	if sig.Action == "BUY" {
		if existingPos, err := s.broker.GetPosition(r.Context(), sig.Symbol); err == nil && existingPos != nil && existingPos.Qty > 0 {
			msg := fmt.Sprintf("duplicate_open_position: %s is already held open (%g shares)", sig.Symbol, existingPos.Qty)
			log.Printf("[Agent Server] Blocking duplicate BUY for %s: %s", sig.SignalID, msg)
			s.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
				"error":     "duplicate_open_position",
				"message":   msg,
				"signal_id": sig.SignalID,
			})
			return
		}
		if partner, ok := safety.PairMap[sig.Symbol]; ok && partner != "" {
			if partnerPos, err := s.broker.GetPosition(r.Context(), partner); err == nil && partnerPos != nil && partnerPos.Qty > 0 {
				msg := fmt.Sprintf("pair_conflict_violation: cannot BUY %s while inverse partner %s is held open (%g shares)", sig.Symbol, partner, partnerPos.Qty)
				log.Printf("[Agent Server] Blocking inverse pair conflict BUY for %s: %s", sig.SignalID, msg)
				rejectedRec := &journal.ExecutionRecord{
					SignalID:        sig.SignalID,
					Broker:          s.broker.Name(),
					ClientOrderID:   sig.SignalID,
					Symbol:          sig.Symbol,
					Action:          sig.Action,
					RequestedShares: sig.Shares,
					SignalPrice:     sig.SignalPrice,
					Status:          "rejected",
					Strategy:        sig.Strategy,
					SubmittedAt:     time.Now().UTC(),
					ErrorMessage:    msg,
					CreatedAt:       time.Now().UTC(),
				}
				_ = s.journal.RecordExecution(r.Context(), rejectedRec)

				s.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
					"error":     "pair_conflict_violation",
					"message":   msg,
					"signal_id": sig.SignalID,
				})
				return
			}
		}

		// 4c. Same-Day Wash Re-Buy Guard: block buying a symbol on the same CST trading day it was already SOLD
		todayCST := time.Now().In(cstLocation).Format("2006-01-02")
		if recentRecs, err := s.journal.GetRecentExecutions(r.Context(), 50); err == nil {
			for _, rec := range recentRecs {
				if rec == nil || !strings.EqualFold(rec.Symbol, sig.Symbol) || !strings.EqualFold(rec.Action, "SELL") {
					continue
				}
				st := strings.ToLower(rec.Status)
				if st != "filled" && st != "submitted" && st != "accepted" && st != "new" && st != "partially_filled" {
					continue
				}
				if rec.SubmittedAt.In(cstLocation).Format("2006-01-02") == todayCST {
					msg := fmt.Sprintf("same_day_wash_rebuy: cannot re-BUY %s on %s after same-day SELL (%s)", sig.Symbol, todayCST, rec.SignalID)
					log.Printf("[Agent Server] Blocking same-day wash re-BUY for %s: %s", sig.SignalID, msg)
					s.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
						"error":     "same_day_wash_rebuy",
						"message":   msg,
						"signal_id": sig.SignalID,
					})
					return
				}
			}
		}
	}

	// 5. Submit order to SignalStack Broker Adapter
	orderReq := &broker.OrderRequest{
		ClientOrderID: sig.SignalID,
		Symbol:        sig.Symbol,
		Side:          sig.Action,
		Type:          "market",
		Qty:           sig.Shares,
		SignalPrice:   sig.SignalPrice,
		TimeInForce:   "day",
	}

	log.Printf("[Agent Server] Submitting order: %s %g %s (Signal ID: %s)",
		sig.Action, sig.Shares, sig.Symbol, sig.SignalID)

	orderResp, err := s.broker.SubmitOrder(r.Context(), orderReq)
	if err != nil {
		log.Printf("[Agent Server] Order submission failed for %s: %v", sig.SignalID, err)
		failedRec := &journal.ExecutionRecord{
			SignalID:        sig.SignalID,
			Broker:          s.broker.Name(),
			ClientOrderID:   sig.SignalID,
			Symbol:          sig.Symbol,
			Action:          sig.Action,
			RequestedShares: sig.Shares,
			SignalPrice:     sig.SignalPrice,
			Status:          "failed",
			Strategy:        sig.Strategy,
			SubmittedAt:     time.Now().UTC(),
			ErrorMessage:    err.Error(),
			CreatedAt:       time.Now().UTC(),
		}
		_ = s.journal.RecordExecution(r.Context(), failedRec)

		s.writeJSON(w, http.StatusBadGateway, map[string]any{
			"error":     "order_submission_failed",
			"message":   err.Error(),
			"signal_id": sig.SignalID,
		})
		return
	}

	// 6. Handle Bootstrap Skip (Graceful ignore of unheld exits)
	if orderResp.Status == "skipped_bootstrap_unheld" {
		skippedRec := &journal.ExecutionRecord{
			SignalID:        sig.SignalID,
			Broker:          s.broker.Name(),
			ClientOrderID:   sig.SignalID,
			Symbol:          sig.Symbol,
			Action:          sig.Action,
			RequestedShares: sig.Shares,
			SignalPrice:     sig.SignalPrice,
			Status:          "skipped_bootstrap_unheld",
			Strategy:        sig.Strategy,
			SubmittedAt:     time.Now().UTC(),
			ErrorMessage:    orderResp.ErrorMessage,
			CreatedAt:       time.Now().UTC(),
		}
		_ = s.journal.RecordExecution(r.Context(), skippedRec)

		s.writeJSON(w, http.StatusOK, map[string]any{
			"status":    "skipped",
			"reason":    "bootstrap_unheld_position",
			"message":   orderResp.ErrorMessage,
			"signal_id": sig.SignalID,
			"symbol":    sig.Symbol,
			"action":    sig.Action,
		})
		return
	}

	// 7. Record Confirmed Execution in Journal (SQLite, PocketBase, Neon)
	rec := &journal.ExecutionRecord{
		SignalID:        sig.SignalID,
		Broker:          s.broker.Name(),
		OrderID:         orderResp.ID,
		ClientOrderID:   sig.SignalID,
		Symbol:          sig.Symbol,
		Action:          sig.Action,
		RequestedShares: sig.Shares,
		FilledShares:    orderResp.FilledQty,
		SignalPrice:     sig.SignalPrice,
		FillPrice:       orderResp.FilledAvgPrice,
		Status:          orderResp.Status,
		Strategy:        sig.Strategy,
		SubmittedAt:     orderResp.SubmittedAt,
		FilledAt:        orderResp.FilledAt,
		RawResponse:     orderResp.RawJSON,
		CreatedAt:       time.Now().UTC(),
	}

	if err := s.journal.RecordExecution(r.Context(), rec); err != nil {
		log.Printf("[Agent Server] Error writing journal for %s: %v", sig.SignalID, err)
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
		"status":          "success",
		"signal_id":       sig.SignalID,
		"broker":          s.broker.Name(),
		"order_id":        orderResp.ID,
		"order_status":    orderResp.Status,
		"symbol":          sig.Symbol,
		"action":          sig.Action,
		"requested_shares": sig.Shares,
		"filled_shares":   orderResp.FilledQty,
		"fill_price":      orderResp.FilledAvgPrice,
		"submitted_at":    orderResp.SubmittedAt.Format(time.RFC3339),
	})
}
