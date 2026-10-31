package server

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"log"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rollingw/signalstack-agent/internal/broker"
	"github.com/rollingw/signalstack-agent/internal/journal"
	"github.com/rollingw/signalstack-agent/internal/safety"
)

var canceledChallengerSignalIDs = map[string]bool{
	"sig-exit-ch-LABU-202610281500-10": true,
	"sig-exit-ch-TNA-202610281500-41":  true,
	"sig-buy-LABD-202610281500-250":    true,
	"sig-buy-SPXS-202610281500-100":    true,
	"sig-buy-TZA-202610281500-50":      true,
	"sig-exit-ch-TZA-202610291100-55":  true,
	"sig-buy-TZA-202610291100-55":      true,
	"sig-buy-ch-TZA-202610291100-55":   true,
}

func isCanceledOrTestSignal(sigID string) bool {
	s := strings.TrimSpace(sigID)
	return canceledChallengerSignalIDs[s] || strings.HasPrefix(s, "sig-test-")
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()

	acct, _ := s.broker.GetAccount(ctx)
	positions, _ := s.broker.GetAllPositions(ctx)
	recent, _ := s.journal.GetRecentExecutions(ctx, 15)
	trades, _ := s.journal.SQLite().GetRecentTrades(ctx, 1000)

	if strings.Contains(r.Header.Get("Accept"), "text/html") {
		var neonMeta *journal.NeonTradeMetadata
		if s.journal.Neon() != nil {
			neonMeta, _ = s.journal.Neon().GetTradeMetadata(ctx)
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, s.renderDashboardHTML(acct, positions, recent, trades, neonMeta))
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

func formatBarCloseCST(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	dt := t.In(cstLocation)
	if dt.Hour() == 7 && dt.Minute() == 0 {
		dt = time.Date(dt.Year(), dt.Month(), dt.Day(), 11, 0, 0, 0, cstLocation)
	}
	return dt.Format("2006-01-02 15:04")
}

func formatFillPrice(val float64) string {
	if val <= 0 {
		return "-"
	}
	rounded2 := math.Round(val*100) / 100
	if math.Abs(val-rounded2) >= 0.0005 {
		s := fmt.Sprintf("%.4f", val)
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
		return "$" + s
	}
	return formatCurrency(val)
}

var (
	tsRegex = regexp.MustCompile(`\b(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::\d{2})?(?:[+-]\d{2}:\d{2}|Z)?\b`)
	reasonPatRegex = regexp.MustCompile(`^(TRAD_HH|TRAD_LH|TRAD_LL|TRAD_HL|ZZ_MACRO_HH_HELD_PAST_TRAD|ZZ_MACRO_LH_HELD_PAST_TRAD|LINEAR_RED_AFTER_TRAD|HH|LH|LL|HL) of (\d{4}-\d{2}-\d{2} \d{2}:\d{2})(.*)$`)
)

func toCSTText(text string) string {
	if text == "" {
		return ""
	}
	return tsRegex.ReplaceAllStringFunc(text, func(match string) string {
		sub := tsRegex.FindStringSubmatch(match)
		if len(sub) < 3 {
			return match
		}
		datePart, timePart := sub[1], sub[2]
		if timePart == "07:00" {
			return datePart + " 11:00"
		}
		if timePart == "03:00" || timePart == "11:00" || timePart == "15:00" || timePart == "19:00" {
			return datePart + " " + timePart
		}
		rawDT, err := time.Parse("2006-01-02 15:04", datePart+" "+timePart)
		if err != nil {
			return match
		}
		cstDT := rawDT.UTC().In(cstLocation)
		if cstDT.Hour() == 7 && cstDT.Minute() == 0 {
			cstDT = time.Date(cstDT.Year(), cstDT.Month(), cstDT.Day(), 11, 0, 0, 0, cstLocation)
		}
		return cstDT.Format("2006-01-02 15:04")
	})
}

func formatTradeReason(reason string, symbol string) string {
	r := strings.TrimSpace(reason)
	if r == "" || r == "-" {
		return "-"
	}
	text := toCSTText(r)
	if symbol != "" {
		if m := reasonPatRegex.FindStringSubmatch(text); len(m) == 4 {
			return fmt.Sprintf("%s of %s (%s)%s", m[1], symbol, m[2], m[3])
		}
	}
	return text
}

func mondayKeyCST(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	dt := t.In(cstLocation)
	offset := (int(dt.Weekday()) + 6) % 7 // Monday = 0
	mon := time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, cstLocation).AddDate(0, 0, -offset)
	return mon.Format("2006-01-02")
}

// ClosedLedgerRow represents a closed trade row for the Challenger M7d Closed Trade Ledger panel.
type ClosedLedgerRow struct {
	ID          string  `json:"id"`
	Symbol      string  `json:"symbol"`
	EntryDate   string  `json:"entry_date"`
	EntryPrice  string  `json:"entry_price"`
	ExitDate    string  `json:"exit_date"`
	ExitTS      int64   `json:"exit_ts"`
	ExitPrice   string  `json:"exit_price"`
	Shares      float64 `json:"shares"`
	Cost        string  `json:"cost"`
	PLVal       float64 `json:"pl_val"`
	PL          string  `json:"pl"`
	PLPctVal    float64 `json:"pl_pct_val"`
	PLPct       string  `json:"pl_pct"`
	EnterReason string  `json:"enter_reason"`
	ExitReason  string  `json:"exit_reason"`
	BarsHeld    int     `json:"bars_held"`
	WeekKey     string  `json:"week_key"`
	IsBroker    bool    `json:"is_broker"`
}

type brokerClosedCycle struct {
	ID          int64
	Symbol      string
	EntryDate   time.Time
	EntryPrice  float64
	ExitDate    time.Time
	ExitPrice   float64
	Shares      float64
	Cost        float64
	PL          float64
	PLPct       float64
	BarsHeld    int
	BuySignal   string
	ExitReason  string
	BuySigID    string
	SellSigID   string
}

func buildClosedTradeLedger(trades []*journal.PaperTrade, neonMeta *journal.NeonTradeMetadata) ([]*ClosedLedgerRow, []string) {
	reasonsBySig := make(map[string]string)
	buySigBySig := make(map[string]string)
	buySigBySymDate := make(map[string]string)
	reasonsBySpan := make(map[string]string)
	reasonsByExit := make(map[string]string)
	barsBySpan := make(map[string]int)
	entryDateBySymExit := make(map[string]time.Time)

	if neonMeta != nil {
		for _, zt := range neonMeta.ZZTrades {
			sym := strings.ToUpper(strings.TrimSpace(zt.Symbol))
			if sym == "" {
				continue
			}
			enDay := ""
			if !zt.EntryDate.IsZero() {
				enDay = zt.EntryDate.In(cstLocation).Format("2006-01-02")
				if zt.BuySignal != "" {
					buySigBySymDate[sym+"|"+enDay] = zt.BuySignal
				}
			}
			if strings.EqualFold(zt.State, "closed") && !zt.ExitDate.IsZero() {
				exDay := zt.ExitDate.In(cstLocation).Format("2006-01-02")
				if !zt.EntryDate.IsZero() {
					entryDateBySymExit[sym+"|"+exDay] = zt.EntryDate
				}
				if enDay != "" && zt.ExitReason != "" {
					reasonsBySpan[sym+"|"+enDay+"|"+exDay] = zt.ExitReason
				}
				if zt.ExitReason != "" {
					if _, exists := reasonsByExit[sym+"|"+exDay]; !exists {
						reasonsByExit[sym+"|"+exDay] = zt.ExitReason
					}
				}
				if enDay != "" && zt.BarsHeld > 0 {
					barsBySpan[sym+"|"+enDay+"|"+exDay] = zt.BarsHeld
				}
			}
		}

		for _, ne := range neonMeta.NotifiedExits {
			sym := strings.ToUpper(strings.TrimSpace(ne.Symbol))
			if sym == "" || ne.ExitDate == "" {
				continue
			}
			if ne.ExitReason != "" {
				if ne.EntryDate != "" {
					reasonsBySpan[sym+"|"+ne.EntryDate+"|"+ne.ExitDate] = ne.ExitReason
				}
				if _, exists := reasonsByExit[sym+"|"+ne.ExitDate]; !exists {
					reasonsByExit[sym+"|"+ne.ExitDate] = ne.ExitReason
				}
			}
			if ne.EntryDate != "" && ne.BarsHeld > 0 {
				barsBySpan[sym+"|"+ne.EntryDate+"|"+ne.ExitDate] = ne.BarsHeld
			}
		}

		for _, ce := range neonMeta.ChallengerExits {
			sym := strings.ToUpper(strings.TrimSpace(ce.Symbol))
			rsn := strings.TrimSpace(ce.ExitReason)
			sig := strings.TrimSpace(ce.SignalID)
			exDay := ce.ExitDate
			if len(exDay) > 10 {
				exDay = exDay[:10]
			}
			if strings.HasPrefix(rsn, "BUY:") {
				cleanBuy := strings.TrimSpace(strings.TrimPrefix(rsn, "BUY:"))
				if sig != "" {
					buySigBySig[sig] = cleanBuy
				}
				if sym != "" && exDay != "" {
					buySigBySymDate[sym+"|"+exDay] = cleanBuy
				}
			} else if rsn != "" {
				if sig != "" {
					reasonsBySig[sig] = rsn
				}
				if sym != "" && exDay != "" {
					reasonsByExit[sym+"|"+exDay] = rsn
				}
			}
		}
	}

	// Sort SQLite paper_trades chronologically ascending
	sortedTrades := make([]*journal.PaperTrade, 0, len(trades))
	for _, t := range trades {
		if t == nil || isCanceledOrTestSignal(t.SignalID) {
			continue
		}
		sortedTrades = append(sortedTrades, t)
	}
	sort.Slice(sortedTrades, func(i, j int) bool {
		if sortedTrades[i].Timestamp.Equal(sortedTrades[j].Timestamp) {
			return sortedTrades[i].ID < sortedTrades[j].ID
		}
		return sortedTrades[i].Timestamp.Before(sortedTrades[j].Timestamp)
	})

	type buyLot struct {
		shares   float64
		price    float64
		dt       time.Time
		signalID string
	}
	inventory := make(map[string][]*buyLot)
	brokerCycles := make([]*brokerClosedCycle, 0)

	for _, t := range sortedTrades {
		sym := strings.ToUpper(strings.TrimSpace(t.Symbol))
		act := strings.ToUpper(strings.TrimSpace(t.Action))
		shs := t.Shares
		px := t.Price
		sig := strings.TrimSpace(t.SignalID)
		rpl := math.Round(t.RealizedPL*100) / 100
		dt := t.Timestamp.In(cstLocation)

		if sym == "" || t.Timestamp.IsZero() {
			continue
		}

		if act == "BUY" {
			inventory[sym] = append(inventory[sym], &buyLot{
				shares:   shs,
				price:    px,
				dt:       dt,
				signalID: sig,
			})
		} else if act == "SELL" {
			remShs := shs
			var buyEntries []*buyLot
			for remShs > 0 && len(inventory[sym]) > 0 {
				lastIdx := len(inventory[sym]) - 1
				b := inventory[sym][lastIdx]
				if b.shares <= remShs {
					buyEntries = append(buyEntries, b)
					remShs -= b.shares
					inventory[sym] = inventory[sym][:lastIdx]
				} else {
					buyEntries = append(buyEntries, &buyLot{
						shares:   remShs,
						price:    b.price,
						dt:       b.dt,
						signalID: b.signalID,
					})
					b.shares -= remShs
					remShs = 0
				}
			}
			inventory[sym] = nil

			totCost := 0.0
			for _, b := range buyEntries {
				totCost += b.shares * b.price
			}
			totCost = math.Round(totCost*100) / 100
			avgEntryPx := 0.0
			var entryDT time.Time
			buySigID := ""
			if shs > 0 && len(buyEntries) > 0 {
				avgEntryPx = math.Round((totCost/shs)*10000) / 10000
				entryDT = buyEntries[0].dt
				buySigID = buyEntries[0].signalID
			}

			if shs > 0 && px > 0 && (rpl != 0 || len(buyEntries) == 0) {
				impliedEP := math.Round((px-(rpl/shs))*10000) / 10000
				if impliedEP > 0 && (avgEntryPx <= 0 || math.Abs(avgEntryPx-impliedEP) > 0.05) {
					avgEntryPx = impliedEP
					totCost = math.Round(shs*impliedEP*100) / 100
				} else if totCost > 0 {
					totCost = math.Round(shs*avgEntryPx*100) / 100
				}
			}

			retPct := 0.0
			if totCost > 0 {
				retPct = math.Round((rpl/totCost)*10000) / 100
			}

			exDay := dt.Format("2006-01-02")
			if entryDT.IsZero() {
				if fallbackEn, ok := entryDateBySymExit[sym+"|"+exDay]; ok {
					entryDT = fallbackEn.In(cstLocation)
				}
			}
			enDay := ""
			if !entryDT.IsZero() {
				enDay = entryDT.In(cstLocation).Format("2006-01-02")
			}

			exitReason := reasonsBySig[sig]
			if exitReason == "" && enDay != "" && exDay != "" {
				exitReason = reasonsBySpan[sym+"|"+enDay+"|"+exDay]
			}
			if exitReason == "" && exDay != "" {
				exitReason = reasonsByExit[sym+"|"+exDay]
			}
			if exitReason == "" {
				exitReason = sig
			}

			buySignal := buySigBySig[buySigID]
			if buySignal == "" && enDay != "" {
				buySignal = buySigBySymDate[sym+"|"+enDay]
			}
			if buySignal == "" {
				buySignal = "Challenger M7d (Intraday T+0) Entry"
			}

			barsHeld := 0
			if enDay != "" && exDay != "" {
				barsHeld = barsBySpan[sym+"|"+enDay+"|"+exDay]
			}
			if barsHeld <= 0 {
				if !entryDT.IsZero() {
					if enDay == exDay {
						hoursDiff := dt.Sub(entryDT).Hours()
						if hoursDiff >= 1.5 {
							barsHeld = int(math.Round(hoursDiff / 2.5))
							if barsHeld < 1 {
								barsHeld = 1
							} else if barsHeld > 3 {
								barsHeld = 3
							}
						} else {
							barsHeld = 1
						}
					} else {
						bDays := 0
						curD := time.Date(entryDT.Year(), entryDT.Month(), entryDT.Day(), 0, 0, 0, 0, cstLocation)
						endD := time.Date(dt.Year(), dt.Month(), dt.Day(), 0, 0, 0, 0, cstLocation)
						for curD.Before(endD) {
							curD = curD.AddDate(0, 0, 1)
							if curD.Weekday() != time.Saturday && curD.Weekday() != time.Sunday {
								bDays++
							}
						}
						barsHeld = bDays * 3
						if barsHeld < 1 {
							barsHeld = 1
						}
					}
				} else {
					barsHeld = 3
				}
			}

			brokerCycles = append(brokerCycles, &brokerClosedCycle{
				ID:         t.ID,
				Symbol:     sym,
				EntryDate:  entryDT,
				EntryPrice: avgEntryPx,
				ExitDate:   dt,
				ExitPrice:  px,
				Shares:     shs,
				Cost:       totCost,
				PL:         rpl,
				PLPct:      retPct,
				BarsHeld:   barsHeld,
				BuySignal:  buySignal,
				ExitReason: exitReason,
				BuySigID:   buySigID,
				SellSigID:  sig,
			})
		}
	}

	brokerBySymExit := make(map[string][]*brokerClosedCycle)
	usedBrokerIDs := make(map[int64]bool)
	for _, bc := range brokerCycles {
		exDay := bc.ExitDate.In(cstLocation).Format("2006-01-02")
		key := bc.Symbol + "|" + exDay
		brokerBySymExit[key] = append(brokerBySymExit[key], bc)
	}

	var rows []*ClosedLedgerRow
	weekSet := make(map[string]bool)

	addRow := func(id string, sym string, enDT time.Time, enPx float64, exDT time.Time, exPx float64, shs float64, cost float64, pl float64, plPct float64, buySig string, exRsn string, bars int, isBroker bool) {
		wk := mondayKeyCST(exDT)
		if wk == "" {
			wk = mondayKeyCST(enDT)
		}
		if wk != "" {
			weekSet[wk] = true
		}
		if bars <= 0 {
			bars = 1
		}
		rows = append(rows, &ClosedLedgerRow{
			ID:          id,
			Symbol:      sym,
			EntryDate:   formatBarCloseCST(enDT),
			EntryPrice:  formatFillPrice(enPx),
			ExitDate:    formatBarCloseCST(exDT),
			ExitTS:      exDT.Unix(),
			ExitPrice:   formatFillPrice(exPx),
			Shares:      shs,
			Cost:        formatCurrency(cost),
			PLVal:       pl,
			PL:          fmt.Sprintf("%+.2f", pl),
			PLPctVal:    plPct,
			PLPct:       fmt.Sprintf("%+.2f%%", plPct),
			EnterReason: formatTradeReason(buySig, sym),
			ExitReason:  formatTradeReason(exRsn, sym),
			BarsHeld:    bars,
			WeekKey:     wk,
			IsBroker:    isBroker,
		})
	}

	if neonMeta != nil && len(neonMeta.ZZTrades) > 0 {
		for _, bt := range neonMeta.ZZTrades {
			if !strings.EqualFold(bt.State, "closed") {
				continue
			}
			sym := strings.ToUpper(strings.TrimSpace(bt.Symbol))
			enDT := bt.EntryDate
			enPx := bt.EntryPrice
			exDT := bt.ExitDate
			exPx := bt.ExitPrice
			shs := bt.Shares
			cost := bt.Cost
			pl := bt.PL
			plPct := bt.PLPct
			buySig := bt.BuySignal
			exRsn := bt.ExitReason
			bars := bt.BarsHeld
			isBroker := false

			exDay := ""
			if !exDT.IsZero() {
				exDay = exDT.In(cstLocation).Format("2006-01-02")
			}
			key := sym + "|" + exDay
			if matches := brokerBySymExit[key]; len(matches) > 0 {
				bc := matches[0]
				brokerBySymExit[key] = matches[1:]
				usedBrokerIDs[bc.ID] = true
				isBroker = true
				if bc.EntryPrice > 0 {
					enPx = bc.EntryPrice
				}
				if bc.ExitPrice > 0 {
					exPx = bc.ExitPrice
				}
				if bc.Shares > 0 {
					shs = bc.Shares
				}
				if bc.Cost > 0 {
					cost = bc.Cost
				}
				pl = bc.PL
				plPct = bc.PLPct
				if bc.ExitReason != "" && !strings.HasPrefix(bc.ExitReason, "sig-") {
					exRsn = bc.ExitReason
				}
			}

			addRow(bt.ID, sym, enDT, enPx, exDT, exPx, shs, cost, pl, plPct, buySig, exRsn, bars, isBroker)
		}
	}

	// Add any Challenger-only broker closed round-trips (or all broker cycles if Neon was unreachable)
	for _, bc := range brokerCycles {
		if usedBrokerIDs[bc.ID] {
			continue
		}
		exRsn := bc.ExitReason
		if exRsn == "" {
			exRsn = "Challenger M7d (Intraday T+0) Exit"
		}
		addRow(
			fmt.Sprintf("ch-%d", bc.ID),
			bc.Symbol,
			bc.EntryDate,
			bc.EntryPrice,
			bc.ExitDate,
			bc.ExitPrice,
			bc.Shares,
			bc.Cost,
			bc.PL,
			bc.PLPct,
			bc.BuySignal,
			exRsn,
			bc.BarsHeld,
			true,
		)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].ExitTS == rows[j].ExitTS {
			return rows[i].Symbol < rows[j].Symbol
		}
		return rows[i].ExitTS > rows[j].ExitTS
	})

	weeks := make([]string, 0, len(weekSet))
	for wk := range weekSet {
		weeks = append(weeks, wk)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(weeks)))

	return rows, weeks
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

func renderClosedLedgerRowHTML(r *ClosedLedgerRow) string {
	plColor := "#94a3b8"
	if r.PLVal > 0 {
		plColor = "#34d399"
	} else if r.PLVal < 0 {
		plColor = "#f87171"
	}
	return fmt.Sprintf(`
		<tr style="border-bottom: 1px solid #1e293b;">
			<td style="padding: 9px 12px; font-weight: 700; color: #ffffff; font-size: 13px; white-space: nowrap;">%s</td>
			<td style="padding: 9px 12px; text-align: center; font-family: monospace; font-size: 12px; color: #e2e8f0; white-space: nowrap;">%s</td>
			<td style="padding: 9px 12px; text-align: right; font-family: monospace; font-size: 12px; color: #38bdf8; font-weight: 600; white-space: nowrap;">%s</td>
			<td style="padding: 9px 12px; text-align: center; font-family: monospace; font-size: 12px; color: #e2e8f0; white-space: nowrap;">%s</td>
			<td style="padding: 9px 12px; text-align: right; font-family: monospace; font-size: 12px; color: #34d399; font-weight: 600; white-space: nowrap;">%s</td>
			<td style="padding: 9px 12px; text-align: center; color: #ffffff; font-weight: 600; font-size: 12px; white-space: nowrap;">%g</td>
			<td style="padding: 9px 12px; text-align: right; font-family: monospace; font-size: 12px; color: #cbd5e1; white-space: nowrap;">%s</td>
			<td style="padding: 9px 12px; text-align: right; font-family: monospace; font-weight: 700; color: %s; font-size: 13px; white-space: nowrap;">%s</td>
			<td style="padding: 9px 12px; text-align: right; font-family: monospace; font-weight: 700; color: %s; font-size: 12px; white-space: nowrap;">%s</td>
			<td style="padding: 9px 12px; font-size: 12px; color: #cbd5e1; white-space: nowrap;">%s</td>
			<td style="padding: 9px 12px; font-size: 12px; color: #cbd5e1; white-space: nowrap;">%s</td>
			<td style="padding: 9px 12px; text-align: center; font-family: monospace; font-size: 12px; color: #94a3b8; white-space: nowrap;">%d</td>
		</tr>`,
		html.EscapeString(r.Symbol),
		html.EscapeString(r.EntryDate),
		html.EscapeString(r.EntryPrice),
		html.EscapeString(r.ExitDate),
		html.EscapeString(r.ExitPrice),
		r.Shares,
		html.EscapeString(r.Cost),
		plColor, html.EscapeString(r.PL),
		plColor, html.EscapeString(r.PLPct),
		html.EscapeString(r.EnterReason),
		html.EscapeString(r.ExitReason),
		r.BarsHeld,
	)
}

func renderClosedLedgerPanel(closedRows []*ClosedLedgerRow, weeks []string) string {
	defaultSymbols := []string{
		"DRIP", "FAS", "FAZ", "GUSH", "LABD", "LABU",
		"SOXL", "SOXS", "SPXL", "SPXS", "TECL", "TECS", "TNA", "TZA",
	}
	symSet := make(map[string]bool)
	for _, s := range defaultSymbols {
		symSet[s] = true
	}
	for _, r := range closedRows {
		if r.Symbol != "" {
			symSet[r.Symbol] = true
		}
	}
	symList := make([]string, 0, len(symSet))
	for s := range symSet {
		symList = append(symList, s)
	}
	sort.Strings(symList)

	var symOptionsHTML strings.Builder
	symOptionsHTML.WriteString(`<option value="ALL">ALL</option>`)
	for _, sym := range symList {
		symOptionsHTML.WriteString(fmt.Sprintf(`<option value="%s">%s</option>`, html.EscapeString(sym), html.EscapeString(sym)))
	}

	var weekOptionsHTML strings.Builder
	weekOptionsHTML.WriteString(`<option value="ALL">All Weeks</option>`)
	weekOptionsHTML.WriteString(`<option value="BROKER_ONLY">Live Broker Fills Only (Post-Bootstrap)</option>`)
	for idx, wStr := range weeks {
		label := "Week " + wStr
		if wDT, err := time.Parse("2006-01-02", wStr); err == nil {
			wEnd := wDT.AddDate(0, 0, 4)
			label = fmt.Sprintf("Week of %s - %s", wDT.Format("Jan 02"), wEnd.Format("Jan 02, 2006"))
			if idx == 0 {
				label += " (Current Week)"
			}
		}
		weekOptionsHTML.WriteString(fmt.Sprintf(`<option value="%s">%s</option>`, html.EscapeString(wStr), html.EscapeString(label)))
	}

	var initialRowsHTML strings.Builder
	if len(closedRows) == 0 {
		initialRowsHTML.WriteString(`<tr><td colspan="12" style="text-align: center; color: #94a3b8; padding: 22px; font-size: 13px;">No closed trades recorded yet</td></tr>`)
	} else {
		limit := 15
		if len(closedRows) < limit {
			limit = len(closedRows)
		}
		for _, r := range closedRows[:limit] {
			initialRowsHTML.WriteString(renderClosedLedgerRowHTML(r))
		}
	}

	jsonBytes, err := json.Marshal(closedRows)
	if err != nil || len(closedRows) == 0 {
		jsonBytes = []byte("[]")
	}

	endIdx := 15
	if len(closedRows) < endIdx {
		endIdx = len(closedRows)
	}
	startIdx := 1
	if len(closedRows) == 0 {
		startIdx = 0
	}

	return fmt.Sprintf(`
		<div class="panel" style="background: #0f172a; border: 1px solid #1e293b; border-radius: 12px; padding: 20px; margin-bottom: 24px;">
			<div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 14px; flex-wrap: wrap; gap: 8px;">
				<div>
					<h2 class="panel-title" style="margin: 0; font-size: 18px; font-weight: 700; color: #ffffff;">Challenger M7d Historical &amp; Live Trade Ledger</h2>
					<div id="closed-ledger-count" style="font-size: 12px; color: #94a3b8; margin-top: 2px;">Showing %d Challenger M7d closed executions reconciled with Fly.io Paper Broker &bull; All times CST</div>
				</div>
				<span style="font-size: 11px; font-weight: 700; color: #a5b4fc; background: #1e1b4b; border: 1px solid #4338ca; padding: 4px 10px; border-radius: 6px;">Closed Trades Ledger</span>
			</div>

			<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 14px; background: #020617; padding: 14px; border-radius: 12px; border: 1px solid #1e293b; margin-bottom: 14px;">
				<div style="display: flex; flex-direction: column; gap: 4px;">
					<label for="ledger-sym-select" style="font-size: 11px; font-weight: 700; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em;">Symbol</label>
					<select id="ledger-sym-select" style="width: 100%%; background: #0f172a; color: #ffffff; border: 1px solid #334155; border-radius: 8px; padding: 8px 12px; font-size: 13px; font-weight: 700; outline: none; cursor: pointer;">
						%s
					</select>
				</div>
				<div style="display: flex; flex-direction: column; gap: 4px;">
					<label for="ledger-week-select" style="font-size: 11px; font-weight: 700; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em;">Week</label>
					<select id="ledger-week-select" style="width: 100%%; background: #0f172a; color: #ffffff; border: 1px solid #334155; border-radius: 8px; padding: 8px 12px; font-size: 13px; font-weight: 700; outline: none; cursor: pointer;">
						%s
					</select>
				</div>
			</div>

			<div style="overflow-x: auto; border-radius: 12px; border: 1px solid #1e293b; background: #020617;">
				<table style="width: 100%%; min-width: 1100px; border-collapse: collapse; text-align: left;">
					<thead>
						<tr style="border-bottom: 1px solid #1e293b; background: #090d16;">
							<th data-sort="symbol" class="ledger-sortable" style="padding: 10px 12px; cursor: pointer; user-select: none; white-space: nowrap;">Symbol <span class="sort-ind"></span></th>
							<th data-sort="entry_date" class="ledger-sortable" style="padding: 10px 12px; text-align: center; cursor: pointer; user-select: none; white-space: nowrap;">Entry Date (CST) <span class="sort-ind"></span></th>
							<th style="padding: 10px 12px; text-align: right; white-space: nowrap;">Broker Entry Fill</th>
							<th data-sort="exit_ts" class="ledger-sortable" style="padding: 10px 12px; text-align: center; cursor: pointer; user-select: none; white-space: nowrap;">Exit Date (CST) <span class="sort-ind">▼</span></th>
							<th style="padding: 10px 12px; text-align: right; white-space: nowrap;">Broker Exit Fill</th>
							<th style="padding: 10px 12px; text-align: center; white-space: nowrap;">Shares</th>
							<th style="padding: 10px 12px; text-align: right; white-space: nowrap;">Broker Cost Basis</th>
							<th data-sort="pl_val" class="ledger-sortable" style="padding: 10px 12px; text-align: right; cursor: pointer; user-select: none; white-space: nowrap;">Realized P/L ($) <span class="sort-ind"></span></th>
							<th data-sort="pl_pct_val" class="ledger-sortable" style="padding: 10px 12px; text-align: right; cursor: pointer; user-select: none; white-space: nowrap;">Return %% <span class="sort-ind"></span></th>
							<th data-sort="enter_reason" class="ledger-sortable" style="padding: 10px 12px; cursor: pointer; user-select: none; white-space: nowrap;">Enter Reason (CST) <span class="sort-ind"></span></th>
							<th data-sort="exit_reason" class="ledger-sortable" style="padding: 10px 12px; cursor: pointer; user-select: none; white-space: nowrap;">Exit Reason (CST) <span class="sort-ind"></span></th>
							<th style="padding: 10px 12px; text-align: center; white-space: nowrap;">Held (Bars)</th>
						</tr>
					</thead>
					<tbody id="closed-ledger-tbody">
						%s
					</tbody>
				</table>
				<div style="display: flex; justify-content: flex-end; align-items: center; gap: 14px; padding: 10px 14px; border-top: 1px solid #1e293b; background: #090d16; font-size: 12px; color: #94a3b8; flex-wrap: wrap;">
					<div style="display: flex; align-items: center; gap: 6px;">
						<span>Records per page:</span>
						<select id="ledger-page-size" style="background: #0f172a; color: #e2e8f0; border: 1px solid #334155; border-radius: 6px; padding: 3px 8px; font-size: 12px; font-weight: 600;">
							<option value="15" selected>15</option>
							<option value="25">25</option>
							<option value="50">50</option>
							<option value="100">100</option>
							<option value="0">All</option>
						</select>
					</div>
					<span id="ledger-page-info">%d-%d of %d</span>
					<div style="display: flex; align-items: center; gap: 4px;">
						<button id="ledger-first-btn" type="button" style="background: #1e293b; color: #e2e8f0; border: 1px solid #334155; border-radius: 6px; padding: 3px 8px; cursor: pointer; font-size: 12px; font-weight: 700;">|&lt;</button>
						<button id="ledger-prev-btn" type="button" style="background: #1e293b; color: #e2e8f0; border: 1px solid #334155; border-radius: 6px; padding: 3px 8px; cursor: pointer; font-size: 12px; font-weight: 700;">&lt;</button>
						<button id="ledger-next-btn" type="button" style="background: #1e293b; color: #e2e8f0; border: 1px solid #334155; border-radius: 6px; padding: 3px 8px; cursor: pointer; font-size: 12px; font-weight: 700;">&gt;</button>
						<button id="ledger-last-btn" type="button" style="background: #1e293b; color: #e2e8f0; border: 1px solid #334155; border-radius: 6px; padding: 3px 8px; cursor: pointer; font-size: 12px; font-weight: 700;">&gt;|</button>
					</div>
				</div>
			</div>
		</div>
		<script>
		(function() {
			var allRows = %s;
			var symSelect = document.getElementById('ledger-sym-select');
			var weekSelect = document.getElementById('ledger-week-select');
			var pageSizeSelect = document.getElementById('ledger-page-size');
			var tbody = document.getElementById('closed-ledger-tbody');
			var countLabel = document.getElementById('closed-ledger-count');
			var pageInfo = document.getElementById('ledger-page-info');
			var firstBtn = document.getElementById('ledger-first-btn');
			var prevBtn = document.getElementById('ledger-prev-btn');
			var nextBtn = document.getElementById('ledger-next-btn');
			var lastBtn = document.getElementById('ledger-last-btn');

			var sortField = 'exit_ts';
			var sortDesc = true;
			var currentPage = 1;

			function esc(s) {
				if (s === null || s === undefined) return '';
				return String(s)
					.replace(/&/g, '&amp;')
					.replace(/</g, '&lt;')
					.replace(/>/g, '&gt;')
					.replace(/"/g, '&quot;');
			}

			function getFilteredRows() {
				var sym = symSelect ? symSelect.value : 'ALL';
				var wk = weekSelect ? weekSelect.value : 'ALL';
				var filtered = allRows.filter(function(r) {
					if (sym !== 'ALL' && r.symbol !== sym) return false;
					if (wk === 'BROKER_ONLY') return !!r.is_broker;
					if (wk !== 'ALL' && r.week_key !== wk) return false;
					return true;
				});
				filtered.sort(function(a, b) {
					var va = a[sortField];
					var vb = b[sortField];
					var cmp = 0;
					if (typeof va === 'number' && typeof vb === 'number') {
						cmp = va - vb;
					} else {
						cmp = String(va || '').localeCompare(String(vb || ''));
					}
					if (cmp === 0) {
						cmp = (b.exit_ts || 0) - (a.exit_ts || 0);
					}
					return sortDesc ? -cmp : cmp;
				});
				return filtered;
			}

			function render() {
				var filtered = getFilteredRows();
				var total = filtered.length;
				if (countLabel) {
					countLabel.innerHTML = 'Showing ' + total + ' Challenger M7d closed executions reconciled with Fly.io Paper Broker &bull; All times CST';
				}
				var pageSize = pageSizeSelect ? parseInt(pageSizeSelect.value, 10) : 15;
				if (!pageSize || pageSize <= 0) pageSize = total || 1;
				var totalPages = Math.max(1, Math.ceil(total / pageSize));
				if (currentPage > totalPages) currentPage = totalPages;
				if (currentPage < 1) currentPage = 1;

				var start = (currentPage - 1) * pageSize;
				var end = Math.min(start + pageSize, total);
				var slice = filtered.slice(start, end);

				if (pageInfo) {
					pageInfo.textContent = (total === 0 ? 0 : start + 1) + '-' + end + ' of ' + total;
				}
				if (firstBtn) firstBtn.disabled = currentPage <= 1;
				if (prevBtn) prevBtn.disabled = currentPage <= 1;
				if (nextBtn) nextBtn.disabled = currentPage >= totalPages;
				if (lastBtn) lastBtn.disabled = currentPage >= totalPages;

				if (!tbody) return;
				if (slice.length === 0) {
					tbody.innerHTML = '<tr><td colspan="12" style="text-align: center; color: #94a3b8; padding: 22px; font-size: 13px;">No closed trades matching selected filters</td></tr>';
					return;
				}
				var htmlParts = [];
				for (var i = 0; i < slice.length; i++) {
					var r = slice[i];
					var plColor = '#94a3b8';
					if (r.pl_val > 0) plColor = '#34d399';
					else if (r.pl_val < 0) plColor = '#f87171';
					htmlParts.push(
						'<tr style="border-bottom: 1px solid #1e293b;">' +
						'<td style="padding: 9px 12px; font-weight: 700; color: #ffffff; font-size: 13px; white-space: nowrap;">' + esc(r.symbol) + '</td>' +
						'<td style="padding: 9px 12px; text-align: center; font-family: monospace; font-size: 12px; color: #e2e8f0; white-space: nowrap;">' + esc(r.entry_date) + '</td>' +
						'<td style="padding: 9px 12px; text-align: right; font-family: monospace; font-size: 12px; color: #38bdf8; font-weight: 600; white-space: nowrap;">' + esc(r.entry_price) + '</td>' +
						'<td style="padding: 9px 12px; text-align: center; font-family: monospace; font-size: 12px; color: #e2e8f0; white-space: nowrap;">' + esc(r.exit_date) + '</td>' +
						'<td style="padding: 9px 12px; text-align: right; font-family: monospace; font-size: 12px; color: #34d399; font-weight: 600; white-space: nowrap;">' + esc(r.exit_price) + '</td>' +
						'<td style="padding: 9px 12px; text-align: center; color: #ffffff; font-weight: 600; font-size: 12px; white-space: nowrap;">' + esc(r.shares) + '</td>' +
						'<td style="padding: 9px 12px; text-align: right; font-family: monospace; font-size: 12px; color: #cbd5e1; white-space: nowrap;">' + esc(r.cost) + '</td>' +
						'<td style="padding: 9px 12px; text-align: right; font-family: monospace; font-weight: 700; color: ' + plColor + '; font-size: 13px; white-space: nowrap;">' + esc(r.pl) + '</td>' +
						'<td style="padding: 9px 12px; text-align: right; font-family: monospace; font-weight: 700; color: ' + plColor + '; font-size: 12px; white-space: nowrap;">' + esc(r.pl_pct) + '</td>' +
						'<td style="padding: 9px 12px; font-size: 12px; color: #cbd5e1; white-space: nowrap;">' + esc(r.enter_reason) + '</td>' +
						'<td style="padding: 9px 12px; font-size: 12px; color: #cbd5e1; white-space: nowrap;">' + esc(r.exit_reason) + '</td>' +
						'<td style="padding: 9px 12px; text-align: center; font-family: monospace; font-size: 12px; color: #94a3b8; white-space: nowrap;">' + esc(r.bars_held) + '</td>' +
						'</tr>'
					);
				}
				tbody.innerHTML = htmlParts.join('');
			}

			if (symSelect) symSelect.addEventListener('change', function() { currentPage = 1; render(); });
			if (weekSelect) weekSelect.addEventListener('change', function() { currentPage = 1; render(); });
			if (pageSizeSelect) pageSizeSelect.addEventListener('change', function() { currentPage = 1; render(); });
			if (firstBtn) firstBtn.addEventListener('click', function() { currentPage = 1; render(); });
			if (prevBtn) prevBtn.addEventListener('click', function() { currentPage--; render(); });
			if (nextBtn) nextBtn.addEventListener('click', function() { currentPage++; render(); });
			if (lastBtn) lastBtn.addEventListener('click', function() {
				var filtered = getFilteredRows();
				var pageSize = pageSizeSelect ? parseInt(pageSizeSelect.value, 10) : 15;
				if (!pageSize || pageSize <= 0) pageSize = filtered.length || 1;
				currentPage = Math.max(1, Math.ceil(filtered.length / pageSize));
				render();
			});

			var headers = document.querySelectorAll('.ledger-sortable');
			headers.forEach(function(th) {
				th.addEventListener('click', function() {
					var f = th.getAttribute('data-sort');
					if (!f) return;
					if (sortField === f) {
						sortDesc = !sortDesc;
					} else {
						sortField = f;
						sortDesc = true;
					}
					headers.forEach(function(h) {
						var ind = h.querySelector('.sort-ind');
						if (ind) ind.textContent = '';
					});
					var activeInd = th.querySelector('.sort-ind');
					if (activeInd) activeInd.textContent = sortDesc ? '▼' : '▲';
					currentPage = 1;
					render();
				});
			});
		})();
		</script>`,
		len(closedRows),
		symOptionsHTML.String(),
		weekOptionsHTML.String(),
		initialRowsHTML.String(),
		startIdx, endIdx, len(closedRows),
		string(jsonBytes),
	)
}

func (s *Server) renderDashboardHTML(
	acct *broker.Account,
	positions []*broker.Position,
	recent []*journal.ExecutionRecord,
	trades []*journal.PaperTrade,
	neonMeta *journal.NeonTradeMetadata,
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
		if t.Action == "SELL" && !isCanceledOrTestSignal(t.SignalID) {
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

	// Closed Trades Ledger Panel HTML (mirrors Challenger M7d Historical & Live Trade Ledger without State dropdown)
	closedLedgerRows, closedWeeks := buildClosedTradeLedger(trades, neonMeta)
	closedLedgerPanelHTML := renderClosedLedgerPanel(closedLedgerRows, closedWeeks)

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

		%s

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
		closedLedgerPanelHTML,
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

	limit := 500
	if q := r.URL.Query().Get("limit"); q != "" {
		if parsed, err := strconv.Atoi(q); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	trades, err := s.journal.SQLite().GetRecentTrades(ctx, limit)
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

		// 4c. Same-Bar Churn Re-Buy Guard: block re-buying a symbol within 60 minutes of a SELL (same 4H bar churn)
		if recentRecs, err := s.journal.GetRecentExecutions(r.Context(), 50); err == nil {
			for _, rec := range recentRecs {
				if rec == nil || !strings.EqualFold(rec.Symbol, sig.Symbol) || !strings.EqualFold(rec.Action, "SELL") {
					continue
				}
				st := strings.ToLower(rec.Status)
				if st != "filled" && st != "submitted" && st != "accepted" && st != "new" && st != "partially_filled" {
					continue
				}
				if !rec.SubmittedAt.IsZero() && time.Since(rec.SubmittedAt) < 60*time.Minute {
					msg := fmt.Sprintf("same_bar_churn_rebuy: cannot re-BUY %s within 60m of SELL (%s at %s)", sig.Symbol, rec.SignalID, rec.SubmittedAt.In(cstLocation).Format(time.RFC3339))
					log.Printf("[Agent Server] Blocking same-bar churn re-BUY for %s: %s", sig.SignalID, msg)
					s.writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
						"error":     "same_bar_churn_rebuy",
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
