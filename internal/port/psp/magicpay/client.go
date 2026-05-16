package magicpay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	baseURL    string
	apiToken   string
	httpClient *http.Client
}

func NewClient(baseURL, apiToken string) *Client {
	return &Client{
		baseURL:    baseURL,
		apiToken:   apiToken,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) do(ctx context.Context, method, path string, body interface{}, result interface{}) error {
	var bodyBytes []byte
	var err error
	if body != nil {
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errResp struct {
			Message string `json:"message"`
		}
		json.NewDecoder(resp.Body).Decode(&errResp)
		return fmt.Errorf("magicpay error %d: %s", resp.StatusCode, errResp.Message)
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}
