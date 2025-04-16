package consumer

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"git.famapp.in/fampay-inc/factlib/pkg/postgres"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// KafkaProducer defines the interface for Kafka producers
type KafkaProducer interface {
	// Produce produces a message to a Kafka topic
	Produce(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) error
	// Close closes the producer
	Close() error
}

// KafkaAdapter implements the KafkaProducer interface
type KafkaAdapter struct {
	client *kgo.Client
	logger *logger.Logger
}

// KafkaConfig represents the configuration for the KafkaAdapter
type KafkaConfig struct {
	BootstrapServers []string
	ClientID         string
	RequiredAcks     int // -1 = all, 1 = leader only, 0 = no acks
}

// NewKafkaAdapter creates a new KafkaAdapter
func NewKafkaAdapter(cfg KafkaConfig, log *logger.Logger) (*KafkaAdapter, error) {
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

	connectTimeout := 30 * time.Second

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapServers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(requiredAcks),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
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
	wg := sync.WaitGroup{}
	wg.Add(1)
	a.client.Produce(bCtx, record, func(r *kgo.Record, err error) {
		if err != nil {
			a.logger.Error("Failed to produce message to Kafka",
				err,
				"topic", topic,
				"key", string(record.Key))
		}
		wg.Done()
		a.logger.Debug("kafka:success", "topic", topic, "key", string(key))
	})
	wg.Wait()
	a.client.Flush(ctx)
	return nil
}

// Close closes the producer
func (a *KafkaAdapter) Close() error {
	a.client.Flush(context.Background())
	a.client.Close()
	return nil
}

// KafkaEventHandler creates an event handler that publishes protobuf events to Kafka
func KafkaEventHandler(producer KafkaProducer) EventHandler {
	loggerConfig := logger.Config{
		Level:      "debug",
		WithCaller: true,
	}
	log := logger.New(loggerConfig)
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
		}

		topic := fmt.Sprintf("%s.%s", event.OutboxPrefix, event.Outbox.AggregateType)
		log.Debug("Attempting to produce message to Kafka",
			"topic", topic,
			"key", string(key))
		if err := producer.Produce(ctx, topic, key, value, headers); err != nil {
			return errors.Wrap(err, "failed to produce message")
		}
		log.Debug("Successfully produced message to Kafka", "event_id", event.Outbox.Id, "topic", topic, "key", string(key))
		return nil
	}
}
