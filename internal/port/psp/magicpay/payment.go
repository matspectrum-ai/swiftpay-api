package magicpay

import (
	"context"
	"fmt"
	"time"

	"github.com/matspectrum/swiftpay-api/internal/domain"
	"github.com/matspectrum/swiftpay-api/internal/port/psp"
)

type createPaymentRequest struct {
	Amount          int64          `json:"amount"`
	Currency        string         `json:"currency"`
	Method          string         `json:"method"`
	Description     string         `json:"description,omitempty"`
	ExternalRef     string         `json:"externalRef"`
	NotificationURL string         `json:"notificationUrl"`
	Payer           *magicPayPayer `json:"payer,omitempty"`
	Items           []magicPayItem `json:"items"`
}

type magicPayPayer struct {
	Name  string `json:"name,omitempty"`
	TaxID string `json:"taxId,omitempty"`
	Email string `json:"email,omitempty"`
	Phone string `json:"phone,omitempty"`
}

type magicPayItem struct {
	Quantity int    `json:"quantity"`
	Name     string `json:"name"`
	Price    int64  `json:"price"`
	Type     string `json:"type"`
}

type paymentResponse struct {
	ID         string         `json:"id"`
	Amount     int64          `json:"amount"`
	Method     string         `json:"method"`
	Currency   string         `json:"currency"`
	Status     string         `json:"status"`
	ExternalRef string        `json:"externalRef"`
	PaidAt     *time.Time     `json:"paidAt"`
	RefundedAt *time.Time     `json:"refundedAt"`
	CreatedAt  time.Time      `json:"createdAt"`
	UpdatedAt  time.Time      `json:"updatedAt"`
	Data       paymentData    `json:"data"`
	Payer      *magicPayPayer `json:"payer"`
}

type paymentData struct {
	Method    string `json:"method"`
	Copypaste string `json:"copypaste"`
	E2E       string `json:"e2e"`
}

type refundResponse struct {
	Message string `json:"message"`
}

func (c *Client) CreateCob(ctx context.Context, txid string, req psp.CobRequest) (*psp.CobResponse, error) {
	amountCents := int64(req.Valor.Original)

	magicReq := createPaymentRequest{
		Amount:          amountCents,
		Currency:        "BRL",
		Method:          "PIX",
		Description:     req.SolPagador,
		ExternalRef:     txid,
		NotificationURL: "https://swiftpay.example.com/api/v1/webhook/callback",
		Items: []magicPayItem{{
			Quantity: 1,
			Name:     "Cobrança Pix",
			Price:    amountCents,
			Type:     "DIGITAL",
		}},
	}

	if req.Devedor.Nome != "" || req.Devedor.CPF != "" || req.Devedor.CNPJ != "" {
		taxID := req.Devedor.CPF
		if taxID == "" {
			taxID = req.Devedor.CNPJ
		}
		magicReq.Payer = &magicPayPayer{
			Name:  req.Devedor.Nome,
			TaxID: taxID,
		}
	}

	var resp paymentResponse
	if err := c.do(ctx, "POST", "/v1/payment", magicReq, &resp); err != nil {
		return nil, fmt.Errorf("magicpay create payment: %w", err)
	}

	return &psp.CobResponse{
		TxID:          resp.ExternalRef,
		Revisao:       0,
		Calendar:      req.Calendar,
		Devedor:       req.Devedor,
		Valor:         req.Valor,
		Chave:         req.Chave,
		SolPagador:    req.SolPagador,
		Status:        domain.CobStatusAtiva,
		Location:      c.baseURL + "/v1/payment/" + resp.ID,
		PixCopiaECola: resp.Data.Copypaste,
	}, nil
}

func (c *Client) UpdateCob(ctx context.Context, txid string, req psp.CobRequest) (*psp.CobResponse, error) {
	return c.CreateCob(ctx, txid, req)
}

func (c *Client) GetCob(ctx context.Context, txid string) (*psp.CobResponse, error) {
	return nil, fmt.Errorf("magicpay: GetCob requires payment ID, not txid")
}

func (c *Client) ListCobs(ctx context.Context, inicio, fim string, limit, offset int) ([]psp.CobResponse, int, error) {
	return nil, 0, fmt.Errorf("magicpay: ListCobs not supported")
}

func (c *Client) GetPayment(ctx context.Context, paymentID string) (*paymentResponse, error) {
	var resp paymentResponse
	if err := c.do(ctx, "GET", "/v1/payment/"+paymentID, nil, &resp); err != nil {
		return nil, fmt.Errorf("magicpay get payment: %w", err)
	}
	return &resp, nil
}

func (c *Client) GetPix(ctx context.Context, e2eid string) (*psp.PixResponse, error) {
	return nil, fmt.Errorf("magicpay: GetPix by e2eid not directly supported — use GetPayment by ID")
}

func (c *Client) ListPix(ctx context.Context, inicio, fim string, limit, offset int) ([]psp.PixResponse, int, error) {
	return nil, 0, fmt.Errorf("magicpay: ListPix not supported")
}

// CreateDevolucao solicita devolução. Idempotency: MagicPay uses the payment ID (e2eid)
// as natural dedup key. Repeated calls with the same e2eid are safe (idempotent).
func (c *Client) CreateDevolucao(ctx context.Context, e2eid, devID, valor string) (*psp.DevolucaoResponse, error) {
	var resp refundResponse
	if err := c.do(ctx, "POST", "/v1/payment/"+e2eid+"/refund", nil, &resp); err != nil {
		return nil, fmt.Errorf("magicpay refund: %w", err)
	}
	return &psp.DevolucaoResponse{
		ID:     devID,
		E2EID:  e2eid,
		Valor:  valor,
		Status: "EM_PROCESSAMENTO",
	}, nil
}

func (c *Client) ConfigureWebhook(ctx context.Context, chave, url string) error {
	return nil
}

func (c *Client) GetWebhook(ctx context.Context, chave string) (*domain.WebhookConfig, error) {
	return &domain.WebhookConfig{
		Chave:  chave,
		Status: "ATIVO",
	}, nil
}

func (c *Client) DeleteWebhook(ctx context.Context, chave string) error {
	return nil
}

var _ psp.PSPClient = (*Client)(nil)
