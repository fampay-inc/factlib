package consumer

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"git.famapp.in/fampay-inc/factlib/pkg/postgres"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"google.golang.org/protobuf/proto"
)

const AckPosKey = "LSN"

// KafkaProducer defines the interface for Kafka producers
type KafkaProducer interface {
	// Produce produces a message to a Kafka topic
	Produce(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) error

	// Send Ack here that broker recevied the msg
	Ack(pos string)
	// Close closes the producer
	Close() error
}

// KafkaAdapter implements the KafkaProducer interface
type KafkaAdapter struct {
	client *kgo.Client
	Acks   chan *string
	logger logger.Logger
}

// KafkaConfig represents the configuration for the KafkaAdapter
type KafkaConfig struct {
	BootstrapServers []string
	ClientID         string
	RequiredAcks     int     // -1 = all, 1 = leader only, 0 = no acks
	SSL              bool    // Enable SSL
	User             *string // SASL user
	Password         *string // SASL password
}

// NewKafkaAdapter creates a new KafkaAdapter
func NewKafkaAdapter(cfg KafkaConfig, log logger.Logger) (*KafkaAdapter, error) {
	if len(cfg.BootstrapServers) == 0 {
		return nil, errors.New("bootstrap servers is required")
	}

	// Default to RequiredAcks = -1 (all) if not specified
	if cfg.RequiredAcks == 0 {
		cfg.RequiredAcks = -1 // -1 means all brokers must acknowledge
	}

	log.Info("Creating Kafka client",
		"bootstrap_servers", cfg.BootstrapServers[0],
		"client_id", cfg.ClientID,
		"required_acks", cfg.RequiredAcks)

	var requiredAcks kgo.Acks
	switch cfg.RequiredAcks {
	case -1:
		requiredAcks = kgo.AllISRAcks()
	case 0:
		requiredAcks = kgo.NoAck()
	case 1:
		requiredAcks = kgo.LeaderAck()
	default:
		requiredAcks = kgo.AllISRAcks() // Default to all ISR acks
	}

	connectTimeout := 120 * time.Second

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapServers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(requiredAcks),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	}
	if cfg.SSL {
		log.Info("Using SSL for Kafka connection")
		opts = append(opts, kgo.SASL(scram.Auth{
			User: *cfg.User,
			Pass: *cfg.Password,
		}.AsSha512Mechanism()))
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{
			MinVersion: tls.VersionTLS12,
		}))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Error("Failed to create Kafka client", err, "bootstrap_servers", cfg.BootstrapServers)
		return nil, errors.Wrap(err, "failed to create Kafka client")
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	pingRecord := &kgo.Record{
		Topic: "_ping",
		Value: []byte("host-" + hostname),
	}
	out := client.ProduceSync(ctx, pingRecord)
	if err := out.FirstErr(); err != nil {
		log.Fatal("ping record error", err)
	} else {
		log.Info("ping record successful")
	}

	return &KafkaAdapter{
		client: client,
		Acks:   make(chan *string, 10),
		logger: log,
	}, nil
}

// Produce produces a message to a Kafka topic
func (a *KafkaAdapter) Produce(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) error {
	a.logger.Debug("kafka:produce message", "topic", topic, "key", string(key))
	// Convert headers to kgo.RecordHeader format
	kafkaHeaders := []kgo.RecordHeader{}
	for k, v := range headers {
		kafkaHeaders = append(kafkaHeaders, kgo.RecordHeader{
			Key:   k,
			Value: []byte(v),
		})
	}

	// Create record
	record := &kgo.Record{
		Topic:   topic,
		Key:     key,
		Value:   value,
		Headers: kafkaHeaders,
	}
	// Produce record with timeout
	bCtx := context.Background()
	a.client.Produce(bCtx, record, func(r *kgo.Record, err error) {
		if err != nil {
			a.logger.Error("Failed to produce message to Kafka",
				err,
				"topic", topic,
				"key", string(record.Key))
			return
		}
		var pos string
		for _, rh := range r.Headers {
			if rh.Key == AckPosKey {
				pos = string(rh.Value)
				a.Ack(pos)
			}
		}
		a.logger.Debug("kafka:success", "topic", topic, "key", string(key), "pos", pos)
	})
	return nil
}

func (a *KafkaAdapter) Ack(pos string) {
	a.logger.Debug("kafka:ack", "pos", pos)
	a.Acks <- &pos
}

// Close closes the producer
func (a *KafkaAdapter) Close() error {
	a.client.Flush(context.Background())
	a.client.Close()
	return nil
}

// KafkaEventHandler creates an event handler that publishes protobuf events to Kafka
func KafkaEventHandler(producer KafkaProducer, logger logger.Logger) EventHandler {
	return func(ctx context.Context, event *postgres.Event) error {
		// Use aggregate ID as the key for partitioning
		key := []byte(event.Outbox.AggregateId)
		// Convert event to protobuf
		value, err := proto.Marshal(&event.Outbox)
		if err != nil {
			return errors.Wrap(err, "failed to marshal proto event")
		}
		// Create headers
		headers := map[string]string{
			"event_id":   event.Outbox.Id,
			"event_type": event.Outbox.EventType,
			"LSN":        event.XLogPos.String(),
		}

		// Trace info
		var traceId string
		if event.Outbox.TraceInfo != nil && event.Outbox.TraceInfo.TraceId != "" {
			traceId = event.Outbox.TraceInfo.TraceId
			headers["trace_id"] = event.Outbox.TraceInfo.TraceId
			headers["span_id"] = event.Outbox.TraceInfo.SpanId
			for k, v := range event.Outbox.TraceInfo.Metadata {
				headers[k] = v
			}
		} else {
			traceId = "unknown"
		}

		topic := fmt.Sprintf("%s.%s", event.OutboxPrefix, event.Outbox.AggregateType)
		logger.Debug("Attempting to produce message to Kafka",
			"topic", topic,
			"key", string(key))
		if err := producer.Produce(ctx, topic, key, value, headers); err != nil {
			logger.Error("failed to produce message to Kafka", "error", err, "event_id", event.Outbox.Id, "topic", topic, "key", string(key), "trace_id", traceId)
			return errors.Wrap(err, "failed to produce message")
		}
		logger.Debug("Successfully produced message to Kafka", "event_id", event.Outbox.Id, "topic", topic, "key", string(key))
		logger.Info("produced", "topic", topic, "event_id", event.Outbox.Id, "trace_id", traceId, "aggregate_id", event.Outbox.AggregateId, "lsn", event.XLogPos.String())
		return nil
	}
}
