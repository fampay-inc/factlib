package metrics

import (
	"errors"
	"fmt"
	"git.famapp.in/fampay-inc/factlib/cmd/owlpost/utils"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	METRICS_ENDPOINT = "/metrics"
)

// StartMetricsServer starts the Prometheus metrics server on the given port and endpoint.
func StartMetricsServer(port int) {
	logger := utils.GetAppLogger()
	go func() {
		logger.Infof("Starting Prometheus metrics server on :%d", port)
		http.Handle(METRICS_ENDPOINT, promhttp.Handler())
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Errorf("Prometheus metrics server error: %v", err)
		}
	}()
}
