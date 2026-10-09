package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the SignalStack Go trading agent.
type Config struct {
	Port                    string
	SignalSecret            string
	SignalStackWebhookURL   string
	SignalStackAPIKey       string
	SignalStackExitAction   string // "sell" or "close"
	InitialCapital          float64
	MarginMultiplier        float64
	BootstrapMode           bool
	BootstrapStartDate      string
	SQLiteDBPath            string
	PocketBaseURL           string
	PocketBaseAdminEmail    string
	PocketBaseAdminPassword string
	NeonDatabaseURL         string
	MaxOrderValue           float64
	MaxSharesPerOrder       float64
	OrderTimeout            time.Duration
	ServiceName             string
	AccountNumber           string
	AccountID               string
	AgentRole               string
	ChampionURL             string
}

// LoadFromEnv loads configuration from environment variables with safe defaults.
func LoadFromEnv() *Config {
	port := getEnv("PORT", "8080")
	signalSecret := getEnv("SIGNAL_SECRET", "super-secret-trading-signal-token-2026")
	webhookURL := getEnv("SIGNALSTACK_WEBHOOK_URL", "")
	apiKey := getEnv("SIGNALSTACK_API_KEY", "")
	exitAction := strings.ToLower(getEnv("SIGNALSTACK_EXIT_ACTION", "sell"))
	initialCapital := getEnvFloat("INITIAL_CAPITAL", 100000.0)
	marginMultiplier := getEnvFloat("MARGIN_MULTIPLIER", 2.0)
	bootstrapMode := getEnvBool("BOOTSTRAP_MODE", true)
	bootstrapStartDate := getEnv("BOOTSTRAP_START_DATE", "2026-09-28")
	sqlitePath := getEnv("SQLITE_DB_PATH", "data/agent.db")
	pbURL := getEnv("POCKETBASE_URL", "")
	pbEmail := getEnv("POCKETBASE_ADMIN_EMAIL", "admin@gain2give.com")
	pbPass := getEnv("POCKETBASE_ADMIN_PASSWORD", "")
	neonURL := getEnv("NEON_DATABASE_URL", "")

	maxOrderValue := getEnvFloat("MAX_ORDER_VALUE", 50000.0)
	maxShares := getEnvFloat("MAX_SHARES_PER_ORDER", 5000.0)
	timeoutSec := getEnvInt("ORDER_TIMEOUT_SEC", 15)

	serviceName := getEnv("SERVICE_NAME", "SignalStack Challenger Trading Agent")
	accountNum := getEnv("ACCOUNT_NUMBER", "SS-CHALLENGER-MAIN")
	accountID := getEnv("ACCOUNT_ID", "signalstack-challenger-001")
	agentRole := getEnv("AGENT_ROLE", "Challenger (Option A: Symmetric Dual-Hybrid)")
	championURL := getEnv("CHAMPION_URL", "https://signalstack-trading-agent.fly.dev")

	return &Config{
		Port:                    port,
		SignalSecret:            signalSecret,
		SignalStackWebhookURL:   webhookURL,
		SignalStackAPIKey:       apiKey,
		SignalStackExitAction:   exitAction,
		InitialCapital:          initialCapital,
		MarginMultiplier:        marginMultiplier,
		BootstrapMode:           bootstrapMode,
		BootstrapStartDate:      bootstrapStartDate,
		SQLiteDBPath:            sqlitePath,
		PocketBaseURL:           pbURL,
		PocketBaseAdminEmail:    pbEmail,
		PocketBaseAdminPassword: pbPass,
		NeonDatabaseURL:         neonURL,
		MaxOrderValue:           maxOrderValue,
		MaxSharesPerOrder:       maxShares,
		OrderTimeout:            time.Duration(timeoutSec) * time.Second,
		ServiceName:             serviceName,
		AccountNumber:           accountNum,
		AccountID:               accountID,
		AgentRole:               agentRole,
		ChampionURL:             championURL,
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		return strings.TrimSpace(val)
	}
	return defaultVal
}

func getEnvFloat(key string, defaultVal float64) float64 {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		if f, err := strconv.ParseFloat(strings.TrimSpace(val), 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		if i, err := strconv.Atoi(strings.TrimSpace(val)); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvBool(key string, defaultVal bool) bool {
	if val, ok := os.LookupEnv(key); ok && strings.TrimSpace(val) != "" {
		v := strings.ToLower(strings.TrimSpace(val))
		return v == "true" || v == "1" || v == "yes"
	}
	return defaultVal
}
