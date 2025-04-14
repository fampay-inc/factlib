package worker

import (
	"context"
	
	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
)

// ExampleHandler is an example implementation of the common.EventHandler interface
type ExampleHandler struct {
	logger *logger.Logger
}

// NewExampleHandler creates a new example handler
func NewExampleHandler(log *logger.Logger) *ExampleHandler {
	return &ExampleHandler{
		logger: log,
	}
}

// Handle handles an event
func (h *ExampleHandler) Handle(ctx context.Context, event common.OutboxEvent) error {
	h.logger.Info("handling event", 
		"id", event.ID, 
		"aggregate_type", event.AggregateType, 
		"aggregate_id", event.AggregateID, 
		"event_type", event.EventType, 
		"payload_size", len(event.Payload))
	
	// Here you would typically do some processing with the event
	// For example, deserialize the payload (which could be a protobuf message)
	// and perform some business logic
	
	return nil
}
