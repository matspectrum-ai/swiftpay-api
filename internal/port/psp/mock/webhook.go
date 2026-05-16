package mock

import (
	"context"
	"fmt"
	"sync"

	"github.com/matspectrum/swiftpay-api/internal/domain"
)

type MockWebhookStore struct {
	mu       sync.RWMutex
	webhooks map[string]*domain.WebhookConfig
}

func NewMockWebhookStore() *MockWebhookStore {
	return &MockWebhookStore{
		webhooks: make(map[string]*domain.WebhookConfig),
	}
}

func (m *MockPSP) ConfigureWebhook(ctx context.Context, chave, url string) error {
	m.webhooks.mu.Lock()
	defer m.webhooks.mu.Unlock()

	m.webhooks.webhooks[chave] = &domain.WebhookConfig{
		Chave:      chave,
		WebhookURL: url,
		Status:     "ATIVO",
	}
	return nil
}

func (m *MockPSP) GetWebhook(ctx context.Context, chave string) (*domain.WebhookConfig, error) {
	m.webhooks.mu.RLock()
	defer m.webhooks.mu.RUnlock()

	wc, ok := m.webhooks.webhooks[chave]
	if !ok {
		return nil, fmt.Errorf("webhook nao encontrado: %s", chave)
	}

	resp := *wc
	return &resp, nil
}

func (m *MockPSP) DeleteWebhook(ctx context.Context, chave string) error {
	m.webhooks.mu.Lock()
	defer m.webhooks.mu.Unlock()

	if _, ok := m.webhooks.webhooks[chave]; !ok {
		return fmt.Errorf("webhook nao encontrado: %s", chave)
	}

	delete(m.webhooks.webhooks, chave)
	return nil
}
