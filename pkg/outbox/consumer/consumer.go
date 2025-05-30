package consumer

import (
	"context"
	"sync"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"git.famapp.in/fampay-inc/factlib/pkg/postgres"
	"github.com/jackc/pglogrepl"
	"github.com/pkg/errors"
)

const (
	defaultPollInterval = 100 * time.Millisecond
)

// OutboxConsumer reads outbox events from PostgreSQL WAL and publishes them to handlers
type OutboxConsumer struct {
	walSubscriber *postgres.WALSubscriber
	logger        logger.Logger
	Handlers      map[string]EventHandler
	handlerAcks   chan *string
	stopCh        chan struct{}
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

// EventHandler handles Protobuf outbox events
type EventHandler func(ctx context.Context, event *postgres.Event) error

// Config represents the configuration for the OutboxConsumer
type Config struct {
	ConnectionString    string
	WalPrefix           string
	ReplicationSlotName string
	PublicationName     string
}

// NewOutboxConsumer creates a new OutboxConsumer
func NewOutboxConsumer(ctx context.Context, cfg Config, log logger.Logger) (*OutboxConsumer, error) {
	if cfg.ConnectionString == "" {
		return nil, errors.New("connection string is required")
	}

	if cfg.WalPrefix == "" {
		return nil, errors.New("prefix is required")
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
		OutboxPrefix:        cfg.WalPrefix,
	}

	// Create WAL subscriber
	walSubscriber, err := postgres.NewWALSubscriber(ctx, walConfig, log)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create WAL subscriber")
	}

	// Create context with cancellation for the consumer
	consumerCtx, cancel := context.WithCancel(context.Background())

	return &OutboxConsumer{
		walSubscriber: walSubscriber,
		logger:        log,
		Handlers:      make(map[string]EventHandler),
		stopCh:        make(chan struct{}),
		ctx:           consumerCtx,
		cancel:        cancel,
	}, nil
}

// RegisterHandler registers a protobuf handler for a specific aggregate type
func (s *OutboxConsumer) RegisterHandler(prefix string, handler EventHandler) {
	s.Handlers[prefix] = handler
}

func (s *OutboxConsumer) RegiserHandlerAck(hAck chan *string) {
	s.handlerAcks = hAck
}

// Start starts the OutboxConsumer
func (s *OutboxConsumer) Start(ctx context.Context) error {
	// Subscribe to WAL events
	events, err := s.walSubscriber.Subscribe(s.ctx)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to WAL events")
	}

	// Start the event processor
	s.wg.Add(2)
	go s.processEvents(events)
	go s.syncAck(ctx)

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
func (s *OutboxConsumer) processEvents(events <-chan *postgres.Event) {
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

			s.logger.Debug("Received event from WAL",
				"id", event.Outbox.Id,
				"aggregate_type", event.Outbox.AggregateType,
				"event_type", event.Outbox.EventType)
			// Handle the event
			s.handleEvent(s.ctx, event)
		}
	}
}

// handleEvent handles a Protobuf event
func (s *OutboxConsumer) handleEvent(ctx context.Context, event *postgres.Event) {
	s.logger.Debug("handle event", "id", event.Outbox.Id,
		"xLogPos", event.XLogPos.String())

	handler, ok := s.Handlers[event.OutboxPrefix]
	if !ok {
		s.logger.Warn("No handler registered for aggregate type", "aggregate_type", event.OutboxPrefix)
		return
	}

	if err := handler(ctx, event); err != nil {
		s.logger.Error("Failed to handle event",
			err,
			"id", event.Outbox.Id,
			"aggregate_type", event.Outbox.AggregateType,
			"event_type", event.Outbox.EventType)
		return
	}

	s.logger.Debug("Successfully processed event", "id", event.Outbox.Id)
}

func (s *OutboxConsumer) syncAck(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Stopping ack synchronization due to context cancellation")
			return
		case ackPos := <-s.handlerAcks:
			if ackPos == nil {
				continue
			}
			lsn, err := pglogrepl.ParseLSN(*ackPos)
			if err != nil {
				s.logger.Fatal("Invalid LSN", err, "lsn", ackPos)
			}
			s.walSubscriber.AckXLogPos <- &lsn
		}
	}
}

func (s *OutboxConsumer) IsWalConsumerHealthy() bool {
	// Check if the WAL subscriber is healthy
	return s.walSubscriber.IsHealthy()
}
