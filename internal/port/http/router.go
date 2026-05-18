package http

import (
	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/matspectrum/swiftpay-api/internal/port/http/handler"
	mw "github.com/matspectrum/swiftpay-api/internal/port/http/middleware"
	"github.com/matspectrum/swiftpay-api/internal/security"
	"github.com/matspectrum/swiftpay-api/internal/store/postgres"
)

type RouterConfig struct {
	HealthHandler   *handler.HealthHandler
	CobHandler      *handler.CobHandler
	PixHandler      *handler.PixHandler
	WebhookHandler  *handler.WebhookHandler
	IdempotencyRepo *postgres.IdempotencyRepo
	RateLimiter     *security.RateLimiter
}

func SetupRouter(cfg RouterConfig) chi.Router {
	r := chi.NewRouter()

	r.Use(chiMiddleware.RealIP)
	r.Use(mw.RequestIDMiddleware)
	r.Use(mw.LoggingMiddleware)
	r.Use(mw.RecoveryMiddleware)
	r.Use(chiMiddleware.CleanPath)
	r.Use(mw.RateLimiterMiddleware(cfg.RateLimiter))

	r.Get("/health", cfg.HealthHandler.Health)
	r.Get("/health/live", cfg.HealthHandler.Liveness)
	r.Get("/health/ready", cfg.HealthHandler.Readiness)

	r.Route("/cob", func(r chi.Router) {
		r.Use(mw.IdempotencyMiddleware(cfg.IdempotencyRepo))
		r.Put("/{txid}", cfg.CobHandler.CreateCob)
		r.Patch("/{txid}", cfg.CobHandler.PatchCob)
		r.Get("/{txid}", cfg.CobHandler.GetCob)
		r.Get("/", cfg.CobHandler.ListCobs)
	})

	r.Route("/pix", func(r chi.Router) {
		r.Get("/{e2eid}", cfg.PixHandler.GetPix)
		r.Get("/", cfg.PixHandler.ListPix)
		r.Put("/{e2eid}/devolucao/{id}", cfg.PixHandler.CreateDevolucao)
	})

	r.Route("/webhook", func(r chi.Router) {
		r.Put("/{chave}", cfg.WebhookHandler.ConfigureWebhook)
		r.Get("/{chave}", cfg.WebhookHandler.GetWebhook)
		r.Get("/", cfg.WebhookHandler.ListWebhooks)
		r.Delete("/{chave}", cfg.WebhookHandler.DeleteWebhook)
	})

	r.Get("/metrics", handler.MetricsHandler().ServeHTTP)

	r.Post("/api/v1/webhook/callback", cfg.WebhookHandler.HandleCallback)

	return r
}
