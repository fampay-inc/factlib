package common

import (
	"context"
	"time"

	pb "git.famapp.in/fampay-inc/factlib/pkg/proto"
)

// OutboxEvent is an alias for the protobuf OutboxEvent
type OutboxEvent = pb.OutboxEvent

// ConvertTimeToUnix converts a time.Time to Unix timestamp (seconds since epoch)
func ConvertTimeToUnix(t time.Time) int64 {
	return t.Unix()
}

// ConvertUnixToTime converts a Unix timestamp to time.Time
func ConvertUnixToTime(unix int64) time.Time {
	return time.Unix(unix, 0).UTC()
}

// OutboxClient defines the interface for publishing events
type OutboxClient interface {
	// Publish publishes an event to the outbox
	Publish(ctx context.Context, aggregateType, aggregateID, eventType string, payload []byte, metadata map[string]string) (string, error)
	
	// Close closes the client
	Close() error
}

// OutboxWorker defines the interface for the worker that processes events
type OutboxWorker interface {
	// Start starts the worker
	Start(ctx context.Context) error
	
	// Register registers a handler for a specific aggregate type
	Register(aggregateType string, topic string, handler EventHandler) error
	
	// Close closes the worker
	Close() error
}

// EventHandler defines the interface for event handlers
type EventHandler interface {
	// Handle handles an event
	Handle(ctx context.Context, event *OutboxEvent) error
}

// KafkaProducer defines the interface for Kafka producers
type KafkaProducer interface {
	// Produce produces a message to a Kafka topic
	Produce(ctx context.Context, topic string, key []byte, value []byte, headers map[string][]byte) error
	
	// Close closes the producer
	Close() error
}

// WALSubscriber defines the interface for WAL subscribers
type WALSubscriber interface {
	// Subscribe subscribes to WAL events
	Subscribe(ctx context.Context) (<-chan OutboxEvent, error)
	
	// Close closes the subscriber
	Close() error
}
