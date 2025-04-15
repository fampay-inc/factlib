package consumer

import (
	"context"
	"sync"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"git.famapp.in/fampay-inc/factlib/pkg/postgres"
	pb "git.famapp.in/fampay-inc/factlib/pkg/proto"
	"github.com/pkg/errors"
)

const (
	defaultPollInterval = 100 * time.Millisecond
)

// OutboxConsumer reads outbox events from PostgreSQL WAL and publishes them to handlers
type OutboxConsumer struct {
	walSubscriber *postgres.WALSubscriber
	logger        *logger.Logger
	handlers      map[string]EventHandler
	protoHandlers map[string]ProtoEventHandler
	stopCh        chan struct{}
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

// EventHandler handles JSON outbox events
type EventHandler func(ctx context.Context, event *common.OutboxEvent) error

// ProtoEventHandler handles Protobuf outbox events
type ProtoEventHandler func(ctx context.Context, event *pb.OutboxEvent) error

// Config represents the configuration for the OutboxConsumer
type Config struct {
	ConnectionString  string
	ProtoPrefix       string
	ReplicationSlotName string
	PublicationName     string
}

// NewOutboxConsumer creates a new OutboxConsumer
func NewOutboxConsumer(ctx context.Context, cfg Config, log *logger.Logger) (*OutboxConsumer, error) {
	if cfg.ConnectionString == "" {
		return nil, errors.New("connection string is required")
	}

	if cfg.ProtoPrefix == "" {
		return nil, errors.New("proto prefix is required")
	}

	// Default values for replication slot and publication if not provided
	replicationSlotName := cfg.ReplicationSlotName
	if replicationSlotName == "" {
		replicationSlotName = "outbox_slot"
	}

	publicationName := cfg.PublicationName
	if publicationName == "" {
		publicationName = "outbox_pub"
	}

	// Create WAL subscriber configuration
	walConfig := postgres.WALConfig{
		DatabaseURL:         cfg.ConnectionString,
		ReplicationSlotName: replicationSlotName,
		PublicationName:     publicationName,
		OutboxPrefix:        cfg.ProtoPrefix,
	}

	// Create WAL subscriber
	walSubscriber, err := postgres.NewWALSubscriber(walConfig, log)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create WAL subscriber")
	}

	// Create context with cancellation for the consumer
	consumerCtx, cancel := context.WithCancel(context.Background())

	return &OutboxConsumer{
		walSubscriber: walSubscriber,
		logger:        log,
		handlers:      make(map[string]EventHandler),
		protoHandlers: make(map[string]ProtoEventHandler),
		stopCh:        make(chan struct{}),
		ctx:           consumerCtx,
		cancel:        cancel,
	}, nil
}

// RegisterHandler registers a handler for a specific aggregate type
func (s *OutboxConsumer) RegisterHandler(aggregateType string, handler EventHandler) {
	s.handlers[aggregateType] = handler
}

// RegisterProtoHandler registers a protobuf handler for a specific aggregate type
func (s *OutboxConsumer) RegisterProtoHandler(aggregateType string, handler ProtoEventHandler) {
	s.protoHandlers[aggregateType] = handler
}

// Start starts the OutboxConsumer
func (s *OutboxConsumer) Start(ctx context.Context) error {
	// Subscribe to WAL events
	events, err := s.walSubscriber.Subscribe(s.ctx)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to WAL events")
	}

	// Start the event processor
	s.wg.Add(1)
	go s.processEvents(events)

	return nil
}

// Stop stops the OutboxConsumer
func (s *OutboxConsumer) Stop() error {
	// Cancel the context to signal all goroutines to stop
	s.cancel()
	
	// Close the stop channel for backward compatibility
	close(s.stopCh)
	
	// Wait for all goroutines to finish
	s.wg.Wait()
	
	// Close the WAL subscriber
	return s.walSubscriber.Close()
}

// processEvents processes events from the WAL subscriber
func (s *OutboxConsumer) processEvents(events <-chan *common.OutboxEvent) {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("Stopping event processor due to context cancellation")
			return
		case <-s.stopCh:
			s.logger.Info("Stopping event processor due to stop channel")
			return
		case event, ok := <-events:
			if !ok {
				s.logger.Info("Event channel closed, stopping processor")
				return
			}
			
			// Process the event
			s.logger.Debug("Received event from WAL", 
				"id", event.Id,
				"aggregate_type", event.AggregateType,
				"event_type", event.EventType)
			
			// Convert to protobuf event
			protoEvent := &pb.OutboxEvent{
				Id:            event.Id,
				AggregateType: event.AggregateType,
				AggregateId:   event.AggregateId,
				EventType:     event.EventType,
				Payload:       event.Payload,
				CreatedAt:     event.CreatedAt,
				Metadata:      event.Metadata,
			}
			
			// Handle the event
			s.handleProtoEvent(s.ctx, protoEvent)
		}
	}
}

// handleProtoEvent handles a Protobuf event
func (s *OutboxConsumer) handleProtoEvent(ctx context.Context, event *pb.OutboxEvent) {
	s.logger.Debug("Processing Protobuf event",
		"id", event.Id,
		"aggregate_type", event.AggregateType,
		"event_type", event.EventType)

	handler, ok := s.protoHandlers[event.AggregateType]
	if !ok {
		s.logger.Warn("No handler registered for aggregate type", "aggregate_type", event.AggregateType)
		return
	}

	if err := handler(ctx, event); err != nil {
		s.logger.Error("Failed to handle event",
			err,
			"id", event.Id,
			"aggregate_type", event.AggregateType,
			"event_type", event.EventType)
		return
	}

	s.logger.Debug("Successfully processed event", "id", event.Id)
}
