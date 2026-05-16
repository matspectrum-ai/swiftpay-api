package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type HTTPTarget struct {
	baseURL string
	client  *http.Client
}

func NewHTTPTarget(baseURL string) *HTTPTarget {
	return &HTTPTarget{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 15 * time.Second},
	}
}

func (h *HTTPTarget) CreatePayment(ctx context.Context, req CreatePaymentRequest) (Payment, error) {
	b, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, h.baseURL+"/payments", bytes.NewReader(b))
	if err != nil {
		return Payment{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(httpReq)
	if err != nil {
		return Payment{}, err
	}
	defer resp.Body.Close()

	var out Payment
	if resp.StatusCode >= 300 {
		return Payment{}, fmt.Errorf("create payment http status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Payment{}, err
	}
	return out, nil
}

func (h *HTTPTarget) GetPayment(ctx context.Context, paymentID string) (Payment, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, h.baseURL+"/payments/"+url.PathEscape(paymentID), nil)
	if err != nil {
		return Payment{}, err
	}

	resp, err := h.client.Do(httpReq)
	if err != nil {
		return Payment{}, err
	}
	defer resp.Body.Close()

	var out Payment
	if resp.StatusCode >= 300 {
		return Payment{}, fmt.Errorf("get payment http status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Payment{}, err
	}
	return out, nil
}

func (h *HTTPTarget) HandleWebhook(ctx context.Context, ev WebhookEvent) error {
	b, _ := json.Marshal(ev)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, h.baseURL+"/webhooks/pix", bytes.NewReader(b))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := h.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook http status %d", resp.StatusCode)
	}
	return nil
}

func (h *HTTPTarget) Reconcile(ctx context.Context, paymentID string) error {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, h.baseURL+"/payments/"+url.PathEscape(paymentID)+"/reconcile", nil)
	if err != nil {
		return err
	}
	resp, err := h.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("reconcile http status %d", resp.StatusCode)
	}
	return nil
}
