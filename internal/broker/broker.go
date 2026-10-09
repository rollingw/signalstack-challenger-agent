package broker

import (
	"context"
	"time"
)

// Account represents a standardized brokerage or paper trading account snapshot.
type Account struct {
	ID             string  `json:"id"`
	AccountNumber  string  `json:"account_number"`
	Status         string  `json:"status"` // e.g. "ACTIVE"
	Currency       string  `json:"currency"`
	Cash           float64 `json:"cash"`
	BuyingPower    float64 `json:"buying_power"`
	Equity         float64 `json:"equity"`
	PortfolioValue float64 `json:"portfolio_value"`
	RealizedPL     float64 `json:"realized_pl"`
	UnrealizedPL   float64 `json:"unrealized_pl"`
	DayPL          float64 `json:"day_pl"`
	TradingBlocked bool    `json:"trading_blocked"`
}

// Position represents an open position for a symbol.
type Position struct {
	Symbol          string    `json:"symbol"`
	Qty             float64   `json:"qty"`
	AvgEntryPrice   float64   `json:"avg_entry_price"`
	BrokerFillPrice float64   `json:"broker_fill_price"` // Authoritative live broker execution price
	CurrentPrice    float64   `json:"current_price"`
	PrevClosePrice  float64   `json:"prev_close_price,omitempty"`
	DayChange       float64   `json:"day_change"`
	DayChangePct    float64   `json:"day_change_pct"`
	DayPL           float64   `json:"day_pl"`
	MarketValue     float64   `json:"market_value"`
	CostBasis       float64   `json:"cost_basis"`
	UnrealizedPL    float64   `json:"unrealized_pl"`
	UnrealizedPLPct float64   `json:"unrealized_pl_pct"`
	OpenedAt        time.Time `json:"opened_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// OrderRequest represents a broker-neutral order submission request.
type OrderRequest struct {
	ClientOrderID string  `json:"client_order_id"`
	Symbol        string  `json:"symbol"`
	Side          string  `json:"side"` // "buy" or "sell"
	Type          string  `json:"type"` // "market" or "limit"
	Qty           float64 `json:"qty"`
	LimitPrice    float64 `json:"limit_price,omitempty"`
	TimeInForce   string  `json:"time_in_force"` // "day", "gtc", etc.
	SignalPrice   float64 `json:"signal_price,omitempty"`
}

// OrderResponse represents the normalized status of an order across brokers.
type OrderResponse struct {
	ID             string     `json:"id"`
	ClientOrderID  string     `json:"client_order_id"`
	Symbol         string     `json:"symbol"`
	Side           string     `json:"side"`
	Type           string     `json:"type"`
	Status         string     `json:"status"` // "filled", "accepted", "rejected", "skipped_bootstrap_unheld"
	RequestedQty   float64    `json:"requested_qty"`
	FilledQty      float64    `json:"filled_qty"`
	FilledAvgPrice float64    `json:"filled_avg_price"`
	SubmittedAt    time.Time  `json:"submitted_at"`
	FilledAt       *time.Time `json:"filled_at,omitempty"`
	ErrorMessage   string     `json:"error_message,omitempty"`
	RawJSON        string     `json:"raw_json,omitempty"`
}

// ReconcilePositionItem represents an authoritative broker position to reconcile.
type ReconcilePositionItem struct {
	Symbol        string  `json:"symbol"`
	Shares        float64 `json:"shares"`
	AvgEntryPrice float64 `json:"avg_entry_price"`
	CurrentPrice  float64 `json:"current_price"`
	PrevClose     float64 `json:"prev_close,omitempty"`
	CostBasis     float64 `json:"cost_basis,omitempty"`
}

// ReconcileOrderFillItem represents an executed broker order fill.
type ReconcileOrderFillItem struct {
	Symbol        string  `json:"symbol"`
	Action        string  `json:"action"` // "BUY" or "SELL"
	Shares        float64 `json:"shares"`
	FillPrice     float64 `json:"fill_price"`
	OrderID       string  `json:"order_id,omitempty"`
	ClientOrderID string  `json:"client_order_id,omitempty"`
}

// Broker defines the standard abstraction for broker adapters.
type Broker interface {
	Name() string
	GetAccount(ctx context.Context) (*Account, error)
	GetPosition(ctx context.Context, symbol string) (*Position, error)
	GetAllPositions(ctx context.Context) ([]*Position, error)
	SubmitOrder(ctx context.Context, req *OrderRequest) (*OrderResponse, error)
	GetOrder(ctx context.Context, orderID string) (*OrderResponse, error)
	UpdateMarketPrice(ctx context.Context, symbol string, price float64, prevClose float64) error
	ReconcilePositions(ctx context.Context, positions []*ReconcilePositionItem, orders []*ReconcileOrderFillItem, cash float64, replace bool) error
}

