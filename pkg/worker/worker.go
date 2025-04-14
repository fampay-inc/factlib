package worker

import (
	"context"
	"sync"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"github.com/pkg/errors"
)

// Config represents the configuration for the worker
type Config struct {
	PollingInterval time.Duration
}

// Worker implements the common.OutboxWorker interface
type Worker struct {
	walSubscriber common.WALSubscriber
	kafkaProducer common.KafkaProducer
	logger        *logger.Logger
	handlers      map[string]*handlerConfig
	mutex         sync.RWMutex
	cfg           Config
}

type handlerConfig struct {
	topic   string
	handler common.EventHandler
}

// NewWorker creates a new worker
func NewWorker(
	walSubscriber common.WALSubscriber, 
	kafkaProducer common.KafkaProducer, 
	log *logger.Logger,
	cfg Config,
) *Worker {
	// Set default values if not provided
	if cfg.PollingInterval == 0 {
		cfg.PollingInterval = 100 * time.Millisecond
	}
	
	return &Worker{
		walSubscriber: walSubscriber,
		kafkaProducer: kafkaProducer,
		logger:        log,
		handlers:      make(map[string]*handlerConfig),
		cfg:           cfg,
	}
}

// Register registers a handler for a specific aggregate type
func (w *Worker) Register(aggregateType string, topic string, handler common.EventHandler) error {
	if aggregateType == "" {
		return errors.New("aggregate type is required")
	}
	if topic == "" {
		return errors.New("topic is required")
	}
	if handler == nil {
		return errors.New("handler is required")
	}
	
	w.mutex.Lock()
	defer w.mutex.Unlock()
	
	if _, exists := w.handlers[aggregateType]; exists {
		return errors.Errorf("handler already registered for aggregate type: %s", aggregateType)
	}
	
	w.handlers[aggregateType] = &handlerConfig{
		topic:   topic,
		handler: handler,
	}
	
	w.logger.Info("registered handler", 
		"aggregate_type", aggregateType, 
		"topic", topic)
	
	return nil
}

// Start starts the worker
func (w *Worker) Start(ctx context.Context) error {
	w.logger.Info("starting worker")
	
	// Subscribe to WAL events
	events, err := w.walSubscriber.Subscribe(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to WAL events")
	}
	
	go w.processEvents(ctx, events)
	
	return nil
}

// Close closes the worker
func (w *Worker) Close() error {
	var errs []error
	
	if err := w.walSubscriber.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to close WAL subscriber"))
	}
	
	if err := w.kafkaProducer.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "failed to close Kafka producer"))
	}
	
	if len(errs) > 0 {
		return errors.Errorf("failed to close worker: %v", errs)
	}
	
	return nil
}

// processEvents processes events from the WAL
func (w *Worker) processEvents(ctx context.Context, events <-chan common.OutboxEvent) {
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("stopping worker", "reason", ctx.Err())
			return
		case event, ok := <-events:
			if !ok {
				w.logger.Info("events channel closed, stopping worker")
				return
			}
			
			if err := w.handleEvent(ctx, event); err != nil {
				w.logger.Error("failed to handle event", err, 
					"id", event.ID, 
					"aggregate_type", event.AggregateType,
					"event_type", event.EventType)
				continue
			}
		}
	}
}

// handleEvent handles a single event
func (w *Worker) handleEvent(ctx context.Context, event common.OutboxEvent) error {
	w.mutex.RLock()
	handlerCfg, exists := w.handlers[event.AggregateType]
	w.mutex.RUnlock()
	
	if !exists {
		w.logger.Debug("no handler registered for aggregate type", 
			"aggregate_type", event.AggregateType)
		return nil
	}
	
	// Call the handler
	if err := handlerCfg.handler.Handle(ctx, event); err != nil {
		return errors.Wrap(err, "handler failed")
	}
	
	// Build the headers
	headers := map[string][]byte{
		"id":            []byte(event.ID),
		"aggregate_type": []byte(event.AggregateType),
		"aggregate_id":  []byte(event.AggregateID),
		"event_type":    []byte(event.EventType),
		"created_at":    []byte(event.CreatedAt.Format(time.RFC3339Nano)),
	}
	
	// Add metadata to headers
	for k, v := range event.Metadata {
		headers["metadata_"+k] = []byte(v)
	}
	
	// Produce to Kafka
	err := w.kafkaProducer.Produce(
		ctx,
		handlerCfg.topic,
		[]byte(event.AggregateID),
		event.Payload,
		headers,
	)
	if err != nil {
		return errors.Wrap(err, "failed to produce to Kafka")
	}
	
	w.logger.Debug("produced event to Kafka", 
		"id", event.ID, 
		"aggregate_type", event.AggregateType, 
		"topic", handlerCfg.topic)
	
	return nil
}
