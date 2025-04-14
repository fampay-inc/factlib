package service

import (
	"context"
	"testing"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	pb "git.famapp.in/fampay-inc/factlib/pkg/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"
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

func (m *MockProtoEventHandler) Handle(ctx context.Context, event *pb.OutboxEvent) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func TestNewOutboxService(t *testing.T) {
	// This test would require a real database connection, so we'll skip it
	t.Skip("Requires a real database connection")
}

func TestRegisterHandler(t *testing.T) {
	// Create a new service
	service := &OutboxService{
		handlers:      make(map[string]EventHandler),
		protoHandlers: make(map[string]ProtoEventHandler),
	}

	// Register a handler
	handler := func(ctx context.Context, event *common.OutboxEvent) error {
		return nil
	}
	service.RegisterHandler("user", handler)

	// Verify the handler was registered
	assert.NotNil(t, service.handlers["user"])
}

func TestRegisterProtoHandler(t *testing.T) {
	// Create a new service
	service := &OutboxService{
		handlers:      make(map[string]EventHandler),
		protoHandlers: make(map[string]ProtoEventHandler),
	}

	// Register a handler
	handler := func(ctx context.Context, event *pb.OutboxEvent) error {
		return nil
	}
	service.RegisterProtoHandler("user", handler)

	// Verify the handler was registered
	assert.NotNil(t, service.protoHandlers["user"])
}

// JSON handling has been removed as part of the refactoring

func TestHandleProtoMessage(t *testing.T) {
	// Create a new service
	log := &logger.Logger{}
	service := &OutboxService{
		logger:        log,
		protoHandlers: make(map[string]ProtoEventHandler),
	}

	// Create a test event
	event := &pb.OutboxEvent{
		Id:           "123",
		AggregateType: "user",
		AggregateId:  "456",
		EventType:    "user_created",
		Payload:      []byte(`{"name":"John"}`),
		CreatedAt:    time.Now().UnixNano(),
		Metadata:     map[string]string{"source": "test"},
	}

	// Register a mock handler with a matcher function
	mockHandler := new(MockProtoEventHandler)
	mockHandler.On("Handle", mock.Anything, mock.MatchedBy(func(e *pb.OutboxEvent) bool {
		return e.Id == event.Id && 
			e.AggregateType == event.AggregateType && 
			e.AggregateId == event.AggregateId && 
			e.EventType == event.EventType
	})).Return(nil)
	
	service.protoHandlers["user"] = mockHandler.Handle

	// Handle the message
	eventProto, _ := proto.Marshal(event)
	service.handleProtoMessage(context.Background(), eventProto)

	// Verify the handler was called
	mockHandler.AssertExpectations(t)
}
