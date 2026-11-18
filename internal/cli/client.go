package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type apiClient struct {
	baseURL string
	token   string
	http    *http.Client
}

type apiError struct {
	Status  int
	Code    string
	Message string
	Details map[string]any
}

// These are transport DTOs for the versioned External API. Keep the complete
// published response shape here because request() deliberately rejects unknown
// response fields; the CLI only consumes the fields required by its command.
type apiWorkflowResponse struct {
	ID              string          `json:"workflow_id"`
	ProjectID       string          `json:"project_id"`
	Name            string          `json:"name"`
	ActiveVersionID json.RawMessage `json:"active_version_id"`
	CreatedBy       string          `json:"created_by"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

type apiDraftRevisionResponse struct {
	ID                  string          `json:"draft_revision_id"`
	ProjectID           string          `json:"project_id"`
	WorkflowID          string          `json:"workflow_id"`
	Revision            int64           `json:"revision_number"`
	IR                  json.RawMessage `json:"ir"`
	CatalogRevision     string          `json:"catalog_revision"`
	CreatedBy           string          `json:"created_by"`
	CreatedAt           time.Time       `json:"created_at"`
	ClonedFromVersionID json.RawMessage `json:"cloned_from_version_id"`
}

type apiDraftResponse struct {
	ProjectID       string                   `json:"project_id"`
	WorkflowID      string                   `json:"workflow_id"`
	CurrentRevision int64                    `json:"current_revision"`
	StateVersion    int64                    `json:"state_version"`
	Current         apiDraftRevisionResponse `json:"current"`
}

type apiPublishedVersionResponse struct {
	ID                    string    `json:"version_id"`
	ProjectID             string    `json:"project_id"`
	WorkflowID            string    `json:"workflow_id"`
	Number                int64     `json:"version_number"`
	SourceDraftRevisionID string    `json:"source_draft_revision_id"`
	ExecutionSnapshotID   string    `json:"execution_snapshot_id"`
	ChangeLog             string    `json:"change_log"`
	CreatedBy             string    `json:"created_by"`
	CreatedAt             time.Time `json:"created_at"`
}

func (err *apiError) Error() string {
	if err.Code == "" {
		return err.Message
	}
	return err.Code + ": " + err.Message
}

func newAPIClient(baseURL, token string, client *http.Client) (*apiClient, error) {
	if strings.TrimSpace(baseURL) == "" || strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("control plane URL and bearer token are required")
	}
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	return &apiClient{baseURL: strings.TrimRight(baseURL, "/"), token: token, http: client}, nil
}

func (client *apiClient) request(ctx context.Context, method, path, idempotencyKey string, input any, output any) error {
	var body io.Reader
	if input != nil {
		encoded, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, client.baseURL+path, body)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+client.token)
	request.Header.Set("Accept", "application/json")
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := client.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, 2<<20)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var envelope struct {
			Error struct {
				Code    string         `json:"code"`
				Message string         `json:"message"`
				Details map[string]any `json:"details"`
			} `json:"error"`
		}
		_ = json.NewDecoder(limited).Decode(&envelope)
		return &apiError{Status: response.StatusCode, Code: envelope.Error.Code, Message: envelope.Error.Message, Details: envelope.Error.Details}
	}
	if output == nil {
		return nil
	}
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(output); err != nil {
		return fmt.Errorf("decode API response: %w", err)
	}
	var trailing any
	if err = decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("API response has trailing JSON")
	}
	return nil
}
