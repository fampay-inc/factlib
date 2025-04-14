package kafka

import (
	"context"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ProducerConfig represents the configuration for the Kafka producer
type ProducerConfig struct {
	Brokers            []string
	ClientID           string
	RequestTimeoutMs   int
	ConnectionTimeoutMs int
	MaxRetries         int
}

// Producer implements the common.KafkaProducer interface
type Producer struct {
	client *kgo.Client
	logger *logger.Logger
}

// NewProducer creates a new Kafka producer
func NewProducer(cfg ProducerConfig, log *logger.Logger) (*Producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RetryBackoffFn(func(attempt int) time.Duration {
			return time.Millisecond * time.Duration(100*(attempt+1))
		}),
	}

	if cfg.RequestTimeoutMs > 0 {
		opts = append(opts, kgo.RequestTimeoutOverhead(time.Duration(cfg.RequestTimeoutMs)*time.Millisecond))
	}

	if cfg.MaxRetries > 0 {
		opts = append(opts, kgo.RetryMax(cfg.MaxRetries))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka client")
	}

	// Initial ping to verify connection
	_, err = client.Metadata(context.Background(), nil)
	if err != nil {
		client.Close()
		return nil, errors.Wrap(err, "failed to connect to Kafka brokers")
	}

	return &Producer{
		client: client,
		logger: log,
	}, nil
}

// Produce produces a message to a Kafka topic
func (p *Producer) Produce(ctx context.Context, topic string, key []byte, value []byte, headers map[string][]byte) error {
	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	// Add headers if provided
	if len(headers) > 0 {
		for k, v := range headers {
			record.Headers = append(record.Headers, kgo.RecordHeader{
				Key:   k,
				Value: v,
			})
		}
	}

	result := p.client.ProduceSync(ctx, record)
	if result.Err != nil {
		p.logger.Error("failed to produce message", result.Err, 
			"topic", topic,
			"partition", result.Partition,
			"offset", result.Offset)
		return errors.Wrap(result.Err, "failed to produce message")
	}

	p.logger.Debug("produced message",
		"topic", topic,
		"partition", result.Partition,
		"offset", result.Offset)

	return nil
}

// Close closes the producer
func (p *Producer) Close() error {
	if p.client != nil {
		p.client.Close()
	}
	return nil
}
