package mock

import (
	"github.com/matspectrum/swiftpay-api/internal/port/psp"
)

type MockPSP struct {
	cobs     *MockCobStore
	pixs     *MockPixStore
	webhooks *MockWebhookStore
}

func NewMockPSP() *MockPSP {
	return &MockPSP{
		cobs:     NewMockCobStore(),
		pixs:     NewMockPixStore(),
		webhooks: NewMockWebhookStore(),
	}
}

var _ psp.PSPClient = (*MockPSP)(nil)

func (m *MockPSP) PixStore() *MockPixStore {
	return m.pixs
}

func (m *MockPSP) CobStore() *MockCobStore {
	return m.cobs
}
