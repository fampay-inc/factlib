package healthHttp

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"git.famapp.in/fampay-inc/factlib/cmd/owlpost/utils"
	"git.famapp.in/fampay-inc/factlib/pkg/outbox/consumer"
)

func WalHealthServer(ctx context.Context, outboxConsumer *consumer.OutboxConsumer, config *utils.Config) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if outboxConsumer.IsWalConsumerHealthy() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Not OK"))
		}
	})

	server := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", config.WALHealthPort),
		Handler: mux,
	}

	// Start server in a goroutine
	go func() {
		utils.GetAppLogger().Infof("Starting health server on port %d", config.WALHealthPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			utils.GetAppLogger().Errorf("Health server error: %v", err)
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	utils.GetAppLogger().Info("Shutting down health server...")

	// Create shutdown context with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		utils.GetAppLogger().Errorf("Health server shutdown error: %v", err)
	} else {
		utils.GetAppLogger().Info("Health server shutdown completed.")
	}
}
