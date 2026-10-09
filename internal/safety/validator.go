package safety

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/rollingw/signalstack-agent/internal/broker"
	"github.com/rollingw/signalstack-agent/internal/config"
)

var symbolRegex = regexp.MustCompile(`^[A-Za-z0-9\.\-\_]{1,16}$`)

// TradeSignal represents the broker-neutral JSON trade instruction from the Python model.
type TradeSignal struct {
	SignalID    string  `json:"signal_id"`
	Symbol      string  `json:"symbol"`
	Action      string  `json:"action"` // "BUY" or "SELL"
	Shares      float64 `json:"shares"`
	SignalPrice float64 `json:"signal_price"`
	Timestamp   string  `json:"timestamp"`
	Strategy    string  `json:"strategy"`
}

// ValidateInstruction verifies the format, types, and values of the signal payload.
func ValidateInstruction(sig *TradeSignal) error {
	sig.SignalID = strings.TrimSpace(sig.SignalID)
	if sig.SignalID == "" {
		return fmt.Errorf("signal_id is required and cannot be empty")
	}
	if len(sig.SignalID) > 128 {
		return fmt.Errorf("signal_id length exceeds maximum 128 characters")
	}

	sig.Symbol = strings.ToUpper(strings.TrimSpace(sig.Symbol))
	if sig.Symbol == "" || !symbolRegex.MatchString(sig.Symbol) {
		return fmt.Errorf("invalid symbol '%s': must be 1-16 alphanumeric characters", sig.Symbol)
	}

	sig.Action = strings.ToUpper(strings.TrimSpace(sig.Action))
	if sig.Action != "BUY" && sig.Action != "SELL" {
		return fmt.Errorf("invalid action '%s': must be BUY or SELL", sig.Action)
	}

	if sig.Shares <= 0 {
		return fmt.Errorf("invalid shares '%g': must be greater than 0", sig.Shares)
	}

	if sig.SignalPrice <= 0 {
		return fmt.Errorf("invalid signal_price '%g': must be greater than 0", sig.SignalPrice)
	}

	sig.Timestamp = strings.TrimSpace(sig.Timestamp)
	if sig.Timestamp != "" {
		parsed := false
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02 15:04:05",
			"2006-01-02 15:04",
			"2006-01-02T15:04:05",
		}
		for _, layout := range formats {
			if _, err := time.Parse(layout, sig.Timestamp); err == nil {
				parsed = true
				break
			}
		}
		if !parsed {
			return fmt.Errorf("unrecognized timestamp format: '%s'", sig.Timestamp)
		}
	} else {
		sig.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	if sig.Strategy == "" {
		sig.Strategy = "direxion_4h_zigzag"
	}

	return nil
}

// EnforceHardSafetyLimits checks account status, order value, buying power, and share limits.
// The Go agent DOES NOT recalculate position size; it solely acts as a gatekeeper.
func EnforceHardSafetyLimits(sig *TradeSignal, acct *broker.Account, cfg *config.Config) error {
	if acct == nil {
		return fmt.Errorf("account details unavailable for safety checks")
	}

	if strings.ToUpper(acct.Status) != "ACTIVE" {
		return fmt.Errorf("paper account is not active (status: %s)", acct.Status)
	}

	if acct.TradingBlocked {
		return fmt.Errorf("trading is currently blocked on account %s", acct.AccountNumber)
	}

	if cfg.MaxSharesPerOrder > 0 && sig.Shares > cfg.MaxSharesPerOrder {
		return fmt.Errorf("order shares (%g) exceeds hard limit of %g shares per order", sig.Shares, cfg.MaxSharesPerOrder)
	}

	estimatedValue := sig.Shares * sig.SignalPrice
	if cfg.MaxOrderValue > 0 && estimatedValue > cfg.MaxOrderValue {
		return fmt.Errorf("estimated order value $%.2f exceeds hard limit of $%.2f", estimatedValue, cfg.MaxOrderValue)
	}

	// For BUY orders, verify available buying power
	if sig.Action == "BUY" {
		if estimatedValue > acct.BuyingPower {
			return fmt.Errorf("insufficient buying power: order requires ~$%.2f but account only has $%.2f buying power",
				estimatedValue, acct.BuyingPower)
		}
	}

	return nil
}
