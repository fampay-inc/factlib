package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	once sync.Once

	EventsEmitted          *prometheus.CounterVec
	EmitFailures           *prometheus.CounterVec
	EventsConsumed         *prometheus.CounterVec
	EventProcessingLatency *prometheus.HistogramVec

	KafkaProduceSuccess  *prometheus.CounterVec
	KafkaProduceFailures *prometheus.CounterVec
	KafkaProduceLatency  *prometheus.HistogramVec

	WALReplicationErrors *prometheus.CounterVec

	EventHandlerSuccess  *prometheus.CounterVec
	EventHandlerFailures *prometheus.CounterVec
	EventHandlerLatency  *prometheus.HistogramVec
)

// Register should be called once by the consuming service.
func Register() {
	once.Do(func() {
		EventsEmitted = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "factlib_events_emitted_total",
				Help: "Total number of events emitted",
			},
			[]string{"aggregate_type", "event_type"},
		)

		EventProcessingLatency = promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "factlib_event_processing_seconds",
				Help:    "Time taken to process one event",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"aggregate_type", "event_type"},
		)

		EmitFailures = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "factlib_emit_failures_total",
				Help: "Total number of failed emits",
			},
			[]string{"aggregate_type", "event_type", "error_type"},
		)

		EventsConsumed = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "factlib_events_consumed_total",
				Help: "Total number of events consumed",
			},
			[]string{"aggregate_type", "event_type"},
		)

		KafkaProduceSuccess = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "factlib_kafka_produce_success_total",
				Help: "Total number of successful Kafka produce calls",
			},
			[]string{"topic"},
		)

		KafkaProduceFailures = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "factlib_kafka_produce_failures_total",
				Help: "Total number of failed Kafka produce calls",
			},
			[]string{"topic", "error_type"},
		)

		KafkaProduceLatency = promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "factlib_kafka_produce_latency_seconds",
				Help:    "Kafka produce latency in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"topic"},
		)

		WALReplicationErrors = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "factlib_wal_replication_errors_total",
				Help: "Total number of WAL replication errors",
			},
			[]string{"slot", "error_type"},
		)

		EventHandlerSuccess = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "factlib_event_handler_success_total",
				Help: "Total number of successful event handler executions",
			},
			[]string{"aggregate_type", "event_type"},
		)

		EventHandlerFailures = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "factlib_event_handler_failures_total",
				Help: "Total number of failed event handler executions",
			},
			[]string{"aggregate_type", "event_type", "error_type"},
		)

		EventHandlerLatency = promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "factlib_event_handler_latency_seconds",
				Help:    "Event handler execution latency in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"aggregate_type", "event_type"},
		)
	})
}
