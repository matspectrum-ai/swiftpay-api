package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
)

type Server struct {
	srv    *http.Server
	router chi.Router
}

func NewServer(port string, router chi.Router) *Server {
	return &Server{
		router: router,
		srv: &http.Server{
			Addr:         fmt.Sprintf(":%s", port),
			Handler:      router,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}
}

func (s *Server) Start(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		slog.InfoContext(ctx, "servidor HTTP iniciado", "addr", s.srv.Addr)
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("servidor HTTP: %w", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		slog.InfoContext(ctx, "sinal recebido, iniciando shutdown", "signal", sig.String())
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown servidor: %w", err)
	}

	slog.InfoContext(ctx, "servidor HTTP parado gracefulmente")
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
