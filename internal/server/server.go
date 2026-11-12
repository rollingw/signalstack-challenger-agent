package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/rollingw/signalstack-agent/internal/broker"
	"github.com/rollingw/signalstack-agent/internal/config"
	"github.com/rollingw/signalstack-agent/internal/journal"
)

const dashboardSessionCookie = "challenger_session"

type Server struct {
	cfg        *config.Config
	broker     *broker.SignalStackBroker
	journal    *journal.MultiJournal
	httpServer *http.Server
}

// NewServer initializes the HTTP server with configured routes and middleware.
func NewServer(cfg *config.Config, b *broker.SignalStackBroker, j *journal.MultiJournal) *Server {
	s := &Server{
		cfg:     cfg,
		broker:  b,
		journal: j,
	}

	mux := http.NewServeMux()

	// Dashboard auth endpoints
	mux.HandleFunc("GET /login", s.handleLoginGet)
	mux.HandleFunc("POST /login", s.handleLoginPost)
	mux.HandleFunc("GET /logout", s.handleLogout)

	// Public inspection endpoints
	mux.HandleFunc("GET /", s.handleRoot)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /account", s.handleStatus)
	mux.HandleFunc("GET /positions", s.handlePositions)
	mux.HandleFunc("GET /journal", s.handleJournal)
	mux.HandleFunc("GET /trades", s.handleTrades)

	// Authenticated endpoints
	mux.HandleFunc("POST /signal", s.authMiddleware(s.handleSignal))
	mux.HandleFunc("POST /price", s.authMiddleware(s.handlePriceUpdate))
	mux.HandleFunc("POST /positions/reconcile", s.authMiddleware(s.handleReconcilePositions))

	s.httpServer = &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      s.loggingMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 20 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s
}

// Start begins listening for incoming HTTP requests.
func (s *Server) Start() error {
	log.Printf("⚡ SignalStack Paper Trading Agent HTTP server listening on port %s", s.cfg.Port)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		duration := time.Since(start)
		if r.URL.Path != "/health" {
			log.Printf("[HTTP] %s %s from %s in %v", r.Method, r.URL.Path, r.RemoteAddr, duration)
		}
	})
}

func (s *Server) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		secretHeader := r.Header.Get("X-Signal-Secret")

		token := ""
		if strings.HasPrefix(authHeader, "Bearer ") {
			token = strings.TrimPrefix(authHeader, "Bearer ")
		} else if secretHeader != "" {
			token = secretHeader
		}

		if token == "" || token != s.cfg.SignalSecret {
			s.writeJSON(w, http.StatusUnauthorized, map[string]any{
				"error":   "unauthorized",
				"message": "Valid Bearer token or X-Signal-Secret required",
			})
			return
		}

		next(w, r)
	}
}

func (s *Server) dashboardExpectedUser() string {
	if s.cfg != nil && strings.TrimSpace(s.cfg.DashboardUsername) != "" {
		return strings.TrimSpace(s.cfg.DashboardUsername)
	}
	return "rollingw"
}

func (s *Server) dashboardExpectedPass() string {
	if s.cfg != nil && strings.TrimSpace(s.cfg.DashboardPassword) != "" {
		return strings.TrimSpace(s.cfg.DashboardPassword)
	}
	return "arcamax1"
}

func (s *Server) computeSessionToken(username string) string {
	secret := "challenger-dashboard-session-secret-2026"
	if s.cfg != nil && s.cfg.SignalSecret != "" {
		secret = s.cfg.SignalSecret
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(username + ":" + s.dashboardExpectedPass()))
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *Server) verifyDashboardCredentials(username, password string) bool {
	u := strings.TrimSpace(username)
	expU := s.dashboardExpectedUser()
	expP := s.dashboardExpectedPass()
	uMatch := subtle.ConstantTimeCompare([]byte(u), []byte(expU)) == 1
	pMatch := subtle.ConstantTimeCompare([]byte(password), []byte(expP)) == 1
	return uMatch && pMatch
}

func (s *Server) isDashboardAuthenticated(r *http.Request) bool {
	expUser := s.dashboardExpectedUser()
	expToken := s.computeSessionToken(expUser)

	if c, err := r.Cookie(dashboardSessionCookie); err == nil && c.Value != "" {
		if subtle.ConstantTimeCompare([]byte(c.Value), []byte(expToken)) == 1 {
			return true
		}
	}

	if u, p, ok := r.BasicAuth(); ok {
		if s.verifyDashboardCredentials(u, p) {
			return true
		}
	}

	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") && s.cfg != nil && s.cfg.SignalSecret != "" {
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.SignalSecret)) == 1 {
			return true
		}
	}

	return false
}

func (s *Server) writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(data)
}
