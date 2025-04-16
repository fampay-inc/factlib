package consumer

import (
	"context"
	"testing"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"git.famapp.in/fampay-inc/factlib/pkg/postgres"
	pb "git.famapp.in/fampay-inc/factlib/pkg/proto"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockPgxConn is a mock for pgx.Conn
type MockPgxConn struct {
	mock.Mock
}

func (m *MockPgxConn) Config() pgx.ConnConfig {
	args := m.Called()
	return args.Get(0).(pgx.ConnConfig)
}

func (m *MockPgxConn) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockPgxConn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	args := m.Called(ctx, sql, arguments)
	return args.Get(0).(pgconn.CommandTag), args.Error(1)
}

func (m *MockPgxConn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	args := m.Called(ctx)
	return args.Get(0).(*pgconn.Notification), args.Error(1)
}

// MockEventHandler is a mock for EventHandler
type MockEventHandler struct {
	mock.Mock
}

func (m *MockEventHandler) Handle(ctx context.Context, event *common.OutboxEvent) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

// MockProtoEventHandler is a mock for ProtoEventHandler
type MockProtoEventHandler struct {
	mock.Mock
}

func (m *MockProtoEventHandler) Handle(ctx context.Context, event *postgres.Event) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func TestNewOutboxConsumer(t *testing.T) {
	// This test would require a real database connection, so we'll skip it
	t.Skip("Requires a real database connection")
}

func TestRegisterProtoHandler(t *testing.T) {
	// Create a new consumer
	consumer := &OutboxConsumer{
		Handlers: make(map[string]ProtoEventHandler),
	}

	// Register a handler
	handler := func(ctx context.Context, event *pb.OutboxEvent) error {
		return nil
	}
	consumer.RegisterProtoHandler("user", handler)

	// Verify the handler was registered
	assert.NotNil(t, consumer.Handlers["user"])
}

// JSON handling has been removed as part of the refactoring

func TestHandleProtoMessage(t *testing.T) {
	// Create a new consumer
	log := &logger.Logger{}
	consumer := &OutboxConsumer{
		logger:   log,
		Handlers: make(map[string]ProtoEventHandler),
	}

	// Create a test event
	ob := pb.OutboxEvent{
		Id:            "123",
		AggregateType: "user",
		AggregateId:   "456",
		EventType:     "user_created",
		Payload:       []byte(`{"name":"John"}`),
		CreatedAt:     time.Now().UnixNano(),
		Metadata:      map[string]string{"source": "test"},
	}
	event := &postgres.Event{
		Outbox:       ob,
		OutboxPrefix: "protobox",
		XLogPos:      pglogrepl.LSN(420),
	}

	// Register a mock handler with a matcher function
	mockHandler := new(MockProtoEventHandler)
	mockHandler.On("Handle", mock.Anything, mock.MatchedBy(func(e *postgres.Event) bool {
		return e.Outbox.Id == event.Outbox.Id &&
			e.Outbox.AggregateType == event.Outbox.AggregateType &&
			e.Outbox.AggregateId == event.Outbox.AggregateId &&
			e.Outbox.EventType == event.Outbox.EventType
	})).Return(nil)

	consumer.Handlers["user"] = mockHandler.Handle

	// Handle the message
	consumer.handleProtoEvent(context.Background(), event)

	// Verify the handler was called
	mockHandler.AssertExpectations(t)
}
