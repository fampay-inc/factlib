package consumer

import (
	"context"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	pb "git.famapp.in/fampay-inc/factlib/pkg/proto"
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

	if cfg.ClientID == "" {
		cfg.ClientID = "outbox-service"
	}

	// Default to RequiredAcks = -1 (all) if not specified
	if cfg.RequiredAcks == 0 {
		cfg.RequiredAcks = -1 // -1 means all brokers must acknowledge
	}

	// Convert required acks to the appropriate constant
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

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapServers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(requiredAcks),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)), // nil uses the default hasher
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka client")
	}

	return &KafkaAdapter{
		client: client,
		logger: log,
	}, nil
}

// Produce produces a message to a Kafka topic
func (a *KafkaAdapter) Produce(ctx context.Context, topic string, key []byte, value []byte, headers map[string]string) error {
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

	// Produce record
	err := a.client.ProduceSync(ctx, record).FirstErr()
	if err != nil {
		return errors.Wrap(err, "failed to produce message")
	}

	a.logger.Debug("message produced",
		"topic", topic,
		"key", string(key))

	return nil
}

// Close closes the producer
func (a *KafkaAdapter) Close() error {
	a.client.Flush(context.Background())
	a.client.Close()
	return nil
}


// KafkaProtoEventHandler creates an event handler that publishes protobuf events to Kafka
func KafkaProtoEventHandler(producer KafkaProducer, topic string) ProtoEventHandler {
	return func(ctx context.Context, event *pb.OutboxEvent) error {
		// Use aggregate ID as the key for partitioning
		key := []byte(event.AggregateId)

		// Convert event to protobuf
		value, err := proto.Marshal(event)
		if err != nil {
			return errors.Wrap(err, "failed to marshal proto event")
		}

		// Create headers
		headers := map[string]string{
			"aggregate_type": event.AggregateType,
			"event_type":     event.EventType,
			"created_at":     time.Unix(0, event.CreatedAt).Format(time.RFC3339),
			"content_type":   "application/protobuf",
		}

		// Add metadata to headers
		for k, v := range event.Metadata {
			headers[k] = v
		}

		// Produce message to Kafka
		if err := producer.Produce(ctx, topic, key, value, headers); err != nil {
			return errors.Wrap(err, "failed to produce message")
		}

		return nil
	}
}
