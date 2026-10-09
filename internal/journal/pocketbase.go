package journal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type PocketBaseJournal struct {
	baseURL    string
	adminEmail string
	adminPass  string
	httpClient *http.Client

	tokenMu  sync.RWMutex
	token    string
	tokenExp time.Time
}

// NewPocketBaseJournal initializes the PocketBase client.
func NewPocketBaseJournal(baseURL, adminEmail, adminPass string) (*PocketBaseJournal, error) {
	if baseURL == "" {
		return nil, nil // PocketBase sync disabled
	}

	pb := &PocketBaseJournal{
		baseURL:    strings.TrimRight(baseURL, "/"),
		adminEmail: adminEmail,
		adminPass:  adminPass,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}

	return pb, nil
}

func (pb *PocketBaseJournal) authenticate(ctx context.Context) (string, error) {
	pb.tokenMu.RLock()
	if pb.token != "" && time.Now().Before(pb.tokenExp) {
		token := pb.token
		pb.tokenMu.RUnlock()
		return token, nil
	}
	pb.tokenMu.RUnlock()

	pb.tokenMu.Lock()
	defer pb.tokenMu.Unlock()

	if pb.token != "" && time.Now().Before(pb.tokenExp) {
		return pb.token, nil
	}

	payload := map[string]string{
		"identity": pb.adminEmail,
		"password": pb.adminPass,
	}
	data, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pb.baseURL+"/api/collections/_superusers/auth-with-password", bytes.NewReader(data))
	if err != nil {
		req, _ = http.NewRequestWithContext(ctx, http.MethodPost, pb.baseURL+"/api/admins/auth-with-password", bytes.NewReader(data))
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := pb.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("pocketbase auth request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("pocketbase auth returned HTTP %d: %s", resp.StatusCode, string(body))
	}

	var authResp struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		return "", fmt.Errorf("failed to parse pocketbase auth response: %w", err)
	}

	pb.token = authResp.Token
	pb.tokenExp = time.Now().Add(12 * time.Hour)
	return pb.token, nil
}

// RecordExecution creates a record in PocketBase execution_journal collection.
func (pb *PocketBaseJournal) RecordExecution(ctx context.Context, rec *ExecutionRecord) error {
	if pb == nil || pb.baseURL == "" {
		return nil
	}

	token, err := pb.authenticate(ctx)
	if err != nil {
		return fmt.Errorf("pocketbase auth error: %w", err)
	}

	bodyMap := map[string]any{
		"signal_id":        rec.SignalID,
		"broker":           rec.Broker,
		"order_id":         rec.OrderID,
		"client_order_id":  rec.ClientOrderID,
		"symbol":           rec.Symbol,
		"action":           rec.Action,
		"requested_shares": rec.RequestedShares,
		"filled_shares":    rec.FilledShares,
		"signal_price":     rec.SignalPrice,
		"fill_price":       rec.FillPrice,
		"status":           rec.Status,
		"strategy":         rec.Strategy,
		"submitted_at":     rec.SubmittedAt.Format(time.RFC3339),
		"error_message":    rec.ErrorMessage,
	}
	if rec.FilledAt != nil {
		bodyMap["filled_at"] = rec.FilledAt.Format(time.RFC3339)
	}

	data, _ := json.Marshal(bodyMap)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pb.baseURL+"/api/collections/execution_journal/records", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", token)

	resp, err := pb.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("pocketbase create record error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("pocketbase create record failed (HTTP %d): %s", resp.StatusCode, string(body))
	}
	return nil
}

// UpdateFill updates fill fields in PocketBase.
func (pb *PocketBaseJournal) UpdateFill(ctx context.Context, rec *ExecutionRecord) error {
	if pb == nil || pb.baseURL == "" {
		return nil
	}

	token, err := pb.authenticate(ctx)
	if err != nil {
		return fmt.Errorf("pocketbase auth error: %w", err)
	}

	filter := url.QueryEscape(fmt.Sprintf(`signal_id="%s"`, rec.SignalID))
	searchURL := fmt.Sprintf("%s/api/collections/execution_journal/records?filter=(%s)&perPage=1", pb.baseURL, filter)

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, searchURL, nil)
	req.Header.Set("Authorization", token)

	resp, err := pb.httpClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		return nil
	}

	var listResp struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&listResp)
	resp.Body.Close()

	if len(listResp.Items) == 0 {
		return nil
	}

	recordID := listResp.Items[0].ID
	updateMap := map[string]any{
		"order_id":      rec.OrderID,
		"filled_shares": rec.FilledShares,
		"fill_price":    rec.FillPrice,
		"status":        rec.Status,
		"error_message": rec.ErrorMessage,
	}
	if rec.FilledAt != nil {
		updateMap["filled_at"] = rec.FilledAt.Format(time.RFC3339)
	}

	data, _ := json.Marshal(updateMap)
	patchReq, _ := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("%s/api/collections/execution_journal/records/%s", pb.baseURL, recordID), bytes.NewReader(data))
	patchReq.Header.Set("Content-Type", "application/json")
	patchReq.Header.Set("Authorization", token)

	patchResp, err := pb.httpClient.Do(patchReq)
	if err != nil {
		return err
	}
	patchResp.Body.Close()
	return nil
}
