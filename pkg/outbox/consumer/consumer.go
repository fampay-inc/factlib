package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	pb "git.famapp.in/fampay-inc/factlib/pkg/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

const (
	defaultPollInterval = 100 * time.Millisecond
)

// OutboxConsumer reads outbox events from PostgreSQL WAL and publishes them to handlers
type OutboxConsumer struct {
	conn          *pgx.Conn
	jsonPrefix    string
	protoPrefix   string
	logger        *logger.Logger
	handlers      map[string]EventHandler
	protoHandlers map[string]ProtoEventHandler
	pollInterval  time.Duration
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// EventHandler handles JSON outbox events
type EventHandler func(ctx context.Context, event *common.OutboxEvent) error

// ProtoEventHandler handles Protobuf outbox events
type ProtoEventHandler func(ctx context.Context, event *pb.OutboxEvent) error

// Config represents the configuration for the OutboxConsumer
type Config struct {
	ConnectionString string
	ProtoPrefix      string
	PollInterval     time.Duration
}

// NewOutboxConsumer creates a new OutboxConsumer
func NewOutboxConsumer(ctx context.Context, cfg Config, log *logger.Logger) (*OutboxConsumer, error) {
	if cfg.ConnectionString == "" {
		return nil, errors.New("connection string is required")
	}

	conn, err := pgx.Connect(ctx, cfg.ConnectionString)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to database")
	}

	protoPrefix := cfg.ProtoPrefix

	pollInterval := cfg.PollInterval
	if pollInterval == 0 {
		pollInterval = defaultPollInterval
	}

	return &OutboxConsumer{
		conn:          conn,
		protoPrefix:   protoPrefix,
		logger:        log,
		handlers:      make(map[string]EventHandler),
		protoHandlers: make(map[string]ProtoEventHandler),
		pollInterval:  pollInterval,
		stopCh:        make(chan struct{}),
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
	// Start the notification listener
	s.wg.Add(1)
	go s.startListener(ctx)

	return nil
}

// Stop stops the OutboxConsumer
func (s *OutboxConsumer) Stop() error {
	close(s.stopCh)
	s.wg.Wait()
	return s.conn.Close(context.Background())
}

// startListener starts the notification listener for logical decoding messages
func (s *OutboxConsumer) startListener(ctx context.Context) {
	defer s.wg.Done()

	// Create a separate connection for listening to notifications
	listenConn, err := pgx.Connect(ctx, s.conn.Config().ConnString())
	if err != nil {
		s.logger.Error("failed to create listener connection", err)
		return
	}
	defer listenConn.Close(context.Background())

	// Listen for notifications on the JSON and Protobuf channels
	_, err = listenConn.Exec(ctx, fmt.Sprintf("LISTEN %s", s.jsonPrefix))
	if err != nil {
		s.logger.Error("failed to listen on JSON channel", err)
		return
	}

	_, err = listenConn.Exec(ctx, fmt.Sprintf("LISTEN %s", s.protoPrefix))
	if err != nil {
		s.logger.Error("failed to listen on Protobuf channel", err)
		return
	}

	s.logger.Info("started notification listener",
		"json_channel", s.jsonPrefix,
		"proto_channel", s.protoPrefix)

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			// Check for notifications
			notification, err := listenConn.WaitForNotification(ctx)
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				s.logger.Error("failed to wait for notification", err)
				return
			}

			// Handle notification based on channel
			switch notification.Channel {
			case s.protoPrefix:
				s.handleProtoMessage(ctx, []byte(notification.Payload))
			}
		}
	}
}

// handleProtoMessage handles a Protobuf message from the notification
func (s *OutboxConsumer) handleProtoMessage(ctx context.Context, content []byte) {
	event := &pb.OutboxEvent{}
	if err := proto.Unmarshal(content, event); err != nil {
		s.logger.Error("failed to unmarshal Protobuf event", err)
		return
	}

	s.logger.Debug("received Protobuf event",
		"id", event.Id,
		"aggregate_type", event.AggregateType,
		"event_type", event.EventType)

	handler, ok := s.protoHandlers[event.AggregateType]
	if !ok {
		s.logger.Warn("no handler registered for aggregate type", "aggregate_type", event.AggregateType)
		return
	}

	if err := handler(ctx, event); err != nil {
		s.logger.Error("failed to handle event",
			err,
			"id", event.Id,
			"aggregate_type", event.AggregateType,
			"event_type", event.EventType)
		return
	}
}
