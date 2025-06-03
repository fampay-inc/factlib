package common

import (
	"errors"
)

type Fact struct {
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       []byte
	Metadata      map[string]string
	TraceInfo     *TraceInfo
}

type TraceInfo struct {
	TraceId  string
	SpanId   string
	ParentOp string
}

func (f *Fact) Validate() error {
	if f.AggregateType == "" {
		return errors.New("aggregate type is required")
	}
	if f.AggregateID == "" {
		return errors.New("aggregate ID is required")
	}
	if f.EventType == "" {
		return errors.New("event type is required")
	}
	return nil
}

func NewFact(aggregateType, aggregateID, eventType string, payload []byte, metadata map[string]string) (*Fact, error) {
	if aggregateType == "" {
		return nil, errors.New("aggregate type is required")
	}
	if aggregateID == "" {
		return nil, errors.New("aggregate ID is required")
	}
	if eventType == "" {
		return nil, errors.New("event type is required")
	}
	return &Fact{
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		EventType:     eventType,
		Payload:       payload,
		Metadata:      metadata,
	}, nil
}
