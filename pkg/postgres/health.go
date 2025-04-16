package postgres

import (
	"context"
	"fmt"
	"net/http"
)

type ConsumerHealth struct {
	isHealthy      bool
	unhealthyCount int
}

func (c *ConsumerHealth) SetHealth(health bool) {
	if health {
		c.isHealthy = true
		c.unhealthyCount = 0
	} else if !health && c.unhealthyCount < 3 {
		c.unhealthyCount++
	} else {
		c.isHealthy = false
	}
}

func (c *ConsumerHealth) Shutdown() {
	c.isHealthy = false
}

func (c *ConsumerHealth) GetHealth() bool {
	return c.isHealthy
}

func (c *ConsumerHealth) HealthHTTPServer(ctx context.Context, config *WALConfig) {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if c.GetHealth() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Not OK"))
		}
	})
	err := http.ListenAndServe(fmt.Sprintf(":%d", config.HealthPort), nil)
	if err != nil {
		// utils.GetAppLogger(ctx).Errorf("Error starting health server: %v", err)
	}
}
