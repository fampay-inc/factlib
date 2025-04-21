package common

import (
	"errors"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/proto"
	"github.com/google/uuid"
)

func NewFact(aggregateType, aggregateID, eventType string, payload []byte, metadata map[string]string) (*proto.OutboxEvent, error) {
	createdAt := time.Now().UTC().UnixNano()
	if aggregateType == "" {
		return nil, errors.New("aggregate type is required")
	}
	if aggregateID == "" {
		return nil, errors.New("aggregate ID is required")
	}
	if eventType == "" {
		return nil, errors.New("event type is required")
	}
	eventId, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	return &proto.OutboxEvent{
		Id:            eventId.String(),
		AggregateType: aggregateType,
		AggregateId:   aggregateID,
		EventType:     eventType,
		Payload:       payload,
		Metadata:      metadata,
		CreatedAt:     createdAt,
	}, nil
}
