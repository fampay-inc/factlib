package common

import (
	"context"
	"time"
)

// OutboxEvent represents an event in the outbox pattern
type OutboxEvent struct {
	ID           string            `json:"id"`
	AggregateType string            `json:"aggregate_type"`
	AggregateID  string            `json:"aggregate_id"`
	EventType    string            `json:"event_type"`
	Payload      []byte            `json:"payload"`
	CreatedAt    time.Time         `json:"created_at"`
	Metadata     map[string]string `json:"metadata"`
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
	Handle(ctx context.Context, event OutboxEvent) error
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
