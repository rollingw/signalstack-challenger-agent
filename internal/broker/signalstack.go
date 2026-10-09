package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// JournalProvider defines the minimal interface required by SignalStackBroker for paper bookkeeping.
type JournalProvider interface {
	GetAccount(ctx context.Context) (*Account, error)
	GetPosition(ctx context.Context, symbol string) (*Position, error)
	GetAllPositions(ctx context.Context) ([]*Position, error)
	ApplyBuy(ctx context.Context, symbol string, shares float64, price float64, signalID string) error
	ApplySell(ctx context.Context, symbol string, shares float64, price float64, signalID string) (float64, error)
	UpdateMarketPrice(ctx context.Context, symbol string, price float64, prevClose float64) error
	ReconcilePositions(ctx context.Context, positions []*ReconcilePositionItem, orders []*ReconcileOrderFillItem, cash float64, replace bool) error
}

type SignalStackBroker struct {
	webhookURL    string
	apiKey        string
	exitAction    string
	bootstrapMode bool
	journal       JournalProvider
	httpClient    *http.Client
}

// NewSignalStackBroker creates a new broker adapter for SignalStack paper trading.
func NewSignalStackBroker(webhookURL, apiKey, exitAction string, bootstrapMode bool, jp JournalProvider) *SignalStackBroker {
	if exitAction == "" {
		exitAction = "sell"
	}
	return &SignalStackBroker{
		webhookURL:    strings.TrimSpace(webhookURL),
		apiKey:        strings.TrimSpace(apiKey),
		exitAction:    strings.ToLower(strings.TrimSpace(exitAction)),
		bootstrapMode: bootstrapMode,
		journal:       jp,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (b *SignalStackBroker) Name() string {
	return "signalstack_paper"
}

func (b *SignalStackBroker) WebhookURL() string {
	return b.webhookURL
}

func (b *SignalStackBroker) BootstrapMode() bool {
	return b.bootstrapMode
}

func (b *SignalStackBroker) GetAccount(ctx context.Context) (*Account, error) {
	if b.journal == nil {
		return nil, fmt.Errorf("journal provider not configured")
	}
	return b.journal.GetAccount(ctx)
}

func (b *SignalStackBroker) GetPosition(ctx context.Context, symbol string) (*Position, error) {
	if b.journal == nil {
		return nil, fmt.Errorf("journal provider not configured")
	}
	return b.journal.GetPosition(ctx, symbol)
}

func (b *SignalStackBroker) GetAllPositions(ctx context.Context) ([]*Position, error) {
	if b.journal == nil {
		return []*Position{}, nil
	}
	return b.journal.GetAllPositions(ctx)
}

func (b *SignalStackBroker) UpdateMarketPrice(ctx context.Context, symbol string, price float64, prevClose float64) error {
	if b.journal == nil {
		return nil
	}
	return b.journal.UpdateMarketPrice(ctx, symbol, price, prevClose)
}

func (b *SignalStackBroker) ReconcilePositions(ctx context.Context, positions []*ReconcilePositionItem, orders []*ReconcileOrderFillItem, cash float64, replace bool) error {
	if b.journal == nil {
		return fmt.Errorf("journal provider not configured")
	}
	return b.journal.ReconcilePositions(ctx, positions, orders, cash, replace)
}

// signalStackWebhookPayload defines the JSON schema sent to SignalStack alert webhook URLs.
type signalStackWebhookPayload struct {
	Symbol       string   `json:"symbol"`
	Action       string   `json:"action"` // "buy", "sell", "close"
	Quantity     float64  `json:"quantity"`
	QuantityType string   `json:"quantity_type,omitempty"` // "fixed"
	LimitPrice   *float64 `json:"limit_price,omitempty"`
}

func (b *SignalStackBroker) SubmitOrder(ctx context.Context, req *OrderRequest) (*OrderResponse, error) {
	now := time.Now().UTC()
	symbol := strings.ToUpper(strings.TrimSpace(req.Symbol))
	side := strings.ToLower(strings.TrimSpace(req.Side))

	fillPrice := req.SignalPrice
	if fillPrice <= 0 && req.LimitPrice > 0 {
		fillPrice = req.LimitPrice
	}
	if fillPrice <= 0 {
		fillPrice = 1.0 // fallback
	}

	// 1. BOOTSTRAP CHECK FOR SELL SIGNALS:
	// If starting fresh from scratch with new trades, do not execute SELL on positions
	// that were opened prior to the bootstrap start and never bought into this agent.
	if side == "sell" && b.bootstrapMode {
		pos, err := b.journal.GetPosition(ctx, symbol)
		if err != nil {
			log.Printf("[SignalStackBroker] Error checking position for %s: %v", symbol, err)
		}
		if pos == nil || pos.Qty <= 0 {
			log.Printf("[SignalStackBroker] 🛡️ Bootstrap Protection: Received SELL for %s (%g shares), but position was not opened during this bootstrap cycle. Skipping order gracefully.",
				symbol, req.Qty)
			return &OrderResponse{
				ID:             fmt.Sprintf("ss-skip-%d", now.UnixNano()),
				ClientOrderID:  req.ClientOrderID,
				Symbol:         symbol,
				Side:           req.Side,
				Type:           req.Type,
				Status:         "skipped_bootstrap_unheld",
				RequestedQty:   req.Qty,
				FilledQty:      0,
				FilledAvgPrice: 0,
				SubmittedAt:    now,
				ErrorMessage:   fmt.Sprintf("Position for %s is not held in bootstrap paper portfolio (opened prior to bootstrap start)", symbol),
			}, nil
		}
	}

	// 2. Prepare SignalStack webhook payload
	actionStr := side
	if side == "sell" && b.exitAction != "" {
		actionStr = b.exitAction
	}
	payload := signalStackWebhookPayload{
		Symbol:       symbol,
		Action:       actionStr,
		Quantity:     req.Qty,
		QuantityType: "fixed",
	}
	if req.LimitPrice > 0 {
		payload.LimitPrice = &req.LimitPrice
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to encode signalstack payload: %w", err)
	}

	orderID := fmt.Sprintf("ss-%d", now.UnixNano())
	rawResp := ""

	// 3. Dispatch to SignalStack Webhook (if configured)
	if b.webhookURL != "" && !strings.EqualFold(b.webhookURL, "dry_run") && !strings.EqualFold(b.webhookURL, "sim") {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, b.webhookURL, bytes.NewReader(payloadBytes))
		if err != nil {
			return nil, fmt.Errorf("failed to create signalstack webhook request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("User-Agent", "SignalStack-Trading-Agent/1.0")

		if b.apiKey != "" {
			httpReq.Header.Set("x-api-key", b.apiKey)
			httpReq.Header.Set("Authorization", "Bearer "+b.apiKey)
		}

		resp, err := b.httpClient.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("signalstack webhook request failed: %w", err)
		}
		defer resp.Body.Close()

		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read signalstack webhook response: %w", err)
		}
		rawResp = string(bodyBytes)

		// SignalStack responds with 200 OK / 201 Created / 202 Accepted on successful receipt
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("signalstack webhook returned HTTP %d: %s", resp.StatusCode, rawResp)
		}

		// Try extracting order ID or message from response
		var parsedResp map[string]any
		if err := json.Unmarshal(bodyBytes, &parsedResp); err == nil {
			if idVal, ok := parsedResp["id"].(string); ok && idVal != "" {
				orderID = idVal
			} else if alertID, ok := parsedResp["alert_id"].(string); ok && alertID != "" {
				orderID = alertID
			}
		}
		log.Printf("[SignalStackBroker] ✅ Successfully dispatched %s %g %s to SignalStack (HTTP %d, Order ID: %s)",
			strings.ToUpper(side), req.Qty, symbol, resp.StatusCode, orderID)
	} else {
		rawResp = `{"status":"simulated","message":"Dry-run simulation mode"}`
		log.Printf("[SignalStackBroker] [DRY RUN / SIMULATION] Executed %s %g %s @ $%.2f (Signal ID: %s)",
			strings.ToUpper(side), req.Qty, symbol, fillPrice, req.ClientOrderID)
	}

	// 4. Update Internal Paper Ledger in SQLite
	if side == "buy" {
		if err := b.journal.ApplyBuy(ctx, symbol, req.Qty, fillPrice, req.ClientOrderID); err != nil {
			log.Printf("[SignalStackBroker] Warning: failed to apply buy to paper ledger: %v", err)
		}
	} else if side == "sell" {
		if _, err := b.journal.ApplySell(ctx, symbol, req.Qty, fillPrice, req.ClientOrderID); err != nil {
			log.Printf("[SignalStackBroker] Warning: failed to apply sell to paper ledger: %v", err)
		}
	}

	filledAt := time.Now().UTC()
	return &OrderResponse{
		ID:             orderID,
		ClientOrderID:  req.ClientOrderID,
		Symbol:         symbol,
		Side:           req.Side,
		Type:           req.Type,
		Status:         "filled",
		RequestedQty:   req.Qty,
		FilledQty:      req.Qty,
		FilledAvgPrice: fillPrice,
		SubmittedAt:    now,
		FilledAt:       &filledAt,
		RawJSON:        rawResp,
	}, nil
}

func (b *SignalStackBroker) GetOrder(ctx context.Context, orderID string) (*OrderResponse, error) {
	// For webhook orders, status is immediately returned on submission
	return &OrderResponse{
		ID:     orderID,
		Status: "filled",
	}, nil
}
