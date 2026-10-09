package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rollingw/signalstack-agent/internal/broker"
	"github.com/rollingw/signalstack-agent/internal/config"
	"github.com/rollingw/signalstack-agent/internal/journal"
	"github.com/rollingw/signalstack-agent/internal/server"
)

func main() {
	log.Println("=========================================================")
	log.Println("⚡ Starting SignalStack Paper Trading Agent")
	log.Println("=========================================================")

	cfg := config.LoadFromEnv()

	// 1. Initialize SQLite Journal (local store, paper ledger & idempotency)
	sqliteJ, err := journal.NewSQLiteJournal(cfg.SQLiteDBPath, cfg.InitialCapital, cfg.MarginMultiplier, cfg.BootstrapStartDate)
	if err != nil {
		log.Fatalf("❌ Failed to initialize SQLite journal: %v", err)
	}
	log.Printf("✅ SQLite paper ledger initialized at: %s (Initial Cash: $%.2f)", cfg.SQLiteDBPath, cfg.InitialCapital)

	// 2. Initialize PocketBase Journal Sync (optional/best-effort)
	var pbJ *journal.PocketBaseJournal
	if cfg.PocketBaseURL != "" {
		pbJ, err = journal.NewPocketBaseJournal(cfg.PocketBaseURL, cfg.PocketBaseAdminEmail, cfg.PocketBaseAdminPassword)
		if err != nil {
			log.Printf("⚠️ PocketBase initialization warning: %v", err)
		} else {
			log.Printf("✅ PocketBase journal sync configured for: %s", cfg.PocketBaseURL)
		}
	}

	// 3. Initialize Neon PostgreSQL Journal Sync (optional/best-effort)
	var neonJ *journal.NeonJournal
	if cfg.NeonDatabaseURL != "" {
		neonJ, err = journal.NewNeonJournal(cfg.NeonDatabaseURL)
		if err != nil {
			log.Printf("⚠️ Neon PostgreSQL initialization warning: %v", err)
		} else {
			log.Println("✅ Neon PostgreSQL journal sync connected and initialized")
		}
	}

	// 4. Initialize MultiJournal Coordinator
	multiJ := journal.NewMultiJournal(sqliteJ, pbJ, neonJ)
	defer multiJ.Close()

	// 5. Initialize Broker Adapter (SignalStack Paper)
	ssBroker := broker.NewSignalStackBroker(cfg.SignalStackWebhookURL, cfg.SignalStackAPIKey, cfg.SignalStackExitAction, cfg.BootstrapMode, sqliteJ)
	if cfg.SignalStackWebhookURL != "" {
		log.Printf("✅ SignalStack Broker initialized with Webhook URL: %s (Exit Action: %s)", cfg.SignalStackWebhookURL, cfg.SignalStackExitAction)
	} else {
		log.Println("ℹ️ SIGNALSTACK_WEBHOOK_URL not set: Operating in Local Simulation / Dry-run Mode")
	}
	if cfg.BootstrapMode {
		log.Println("🚀 Bootstrap Mode ACTIVE: Agent starts with 0 historical open trades. Unheld exit signals will be safely ignored.")
	}

	// 6. Initialize HTTP Server
	srv := server.NewServer(cfg, ssBroker, multiJ)

	// 7. Handle Graceful Shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := srv.Start(); err != nil && err.Error() != "http: Server closed" {
			log.Fatalf("❌ Server failure: %v", err)
		}
	}()

	<-stop
	log.Println("🛑 Shutting down SignalStack Paper Trading Agent...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("⚠️ Server shutdown error: %v", err)
	}

	log.Println("👋 SignalStack Trading Agent stopped cleanly.")
}
