package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type REST struct {
	Base string
	HTTP *http.Client
}

func (r *REST) Append(ctx context.Context, tenantID, sessionID string, event map[string]any, fetchN int) error {
	body := map[string]any{"event": event, "fetch_last_n": fetchN}
	buf, _ := json.Marshal(body)
	url := fmt.Sprintf("%s/api/tenants/%s/sessions/%s/append_and_fetch", r.Base, tenantID, sessionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("append: %s", resp.Status)
	}
	return nil
}

func (r *REST) Fork(ctx context.Context, tenantID, parent, child string) error {
	body := map[string]any{"tenant_id": tenantID, "new_session_id": child}
	buf, _ := json.Marshal(body)
	url := fmt.Sprintf("%s/api/sessions/%s/fork", r.Base, parent)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("fork: %s", resp.Status)
	}
	return nil
}
