package client

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// MockConnection implements the Connection interface for testing
type MockConnection struct {
	execOutboxFunc func(ctx context.Context, event common.OutboxEvent) error
}

func (m *MockConnection) ExecOutboxEvent(ctx context.Context, event common.OutboxEvent) error {
	if m.execOutboxFunc != nil {
		return m.execOutboxFunc(ctx, event)
	}
	return nil
}

func TestEmitEvent(t *testing.T) {
	tests := []struct {
		name          string
		aggregateType string
		aggregateID   string
		eventType     string
		payload       []byte
		metadata      map[string]string
		execFunc      func(ctx context.Context, event common.OutboxEvent) error
		wantErr       bool
	}{
		{
			name:          "successful emit",
			aggregateType: "user",
			aggregateID:   "123",
			eventType:     "user.created",
			payload:       []byte(`{"id":"123","name":"test"}`),
			metadata:      map[string]string{"source": "api"},
			execFunc:      func(ctx context.Context, event common.OutboxEvent) error { return nil },
			wantErr:       false,
		},
		{
			name:          "missing aggregate type",
			aggregateType: "",
			aggregateID:   "123",
			eventType:     "user.created",
			payload:       []byte(`{"id":"123","name":"test"}`),
			metadata:      map[string]string{"source": "api"},
			execFunc:      func(ctx context.Context, event common.OutboxEvent) error { return nil },
			wantErr:       true,
		},
		{
			name:          "missing aggregate ID",
			aggregateType: "user",
			aggregateID:   "",
			eventType:     "user.created",
			payload:       []byte(`{"id":"123","name":"test"}`),
			metadata:      map[string]string{"source": "api"},
			execFunc:      func(ctx context.Context, event common.OutboxEvent) error { return nil },
			wantErr:       true,
		},
		{
			name:          "missing event type",
			aggregateType: "user",
			aggregateID:   "123",
			eventType:     "",
			payload:       []byte(`{"id":"123","name":"test"}`),
			metadata:      map[string]string{"source": "api"},
			execFunc:      func(ctx context.Context, event common.OutboxEvent) error { return nil },
			wantErr:       true,
		},
		{
			name:          "conn error",
			aggregateType: "user",
			aggregateID:   "123",
			eventType:     "user.created",
			payload:       []byte(`{"id":"123","name":"test"}`),
			metadata:      map[string]string{"source": "api"},
			execFunc:      func(ctx context.Context, event common.OutboxEvent) error { return context.DeadlineExceeded },
			wantErr:       true,
		},
	}

	// Logger is unused in this test but would typically be passed to components

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockConn := &MockConnection{
				execOutboxFunc: tt.execFunc,
			}

			_, err := EmitEvent(
				context.Background(),
				mockConn,
				tt.aggregateType,
				tt.aggregateID,
				tt.eventType,
				tt.payload,
				tt.metadata,
			)

			if (err != nil) != tt.wantErr {
				t.Errorf("EmitEvent() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestPgxTxAdapter_ExecOutboxEvent(t *testing.T) {
	// Test that event generation works correctly
	pgxTxAdapter := &PgxTxAdapter{
		prefix: "test_prefix",
		logger: logger.New(logger.Config{
			Level:      "info",
			JSONOutput: false,
			WithCaller: false,
		}),
	}

	// Create a test event
	event := common.OutboxEvent{
		ID:           "",
		AggregateType: "user",
		AggregateID:  "123",
		EventType:    "user.created",
		Payload:      []byte(`{"id":"123","name":"test"}`),
		CreatedAt:    time.Time{},
		Metadata:     map[string]string{"source": "api"},
	}

	// Mock the transaction's Exec method
	pgxTxAdapter.tx = &mockPgxTx{
		execFunc: func(ctx context.Context, sql string, args ...interface{}) (int64, error) {
			// Verify the SQL statement contains our prefix
			if sql != "SELECT pg_logical_emit_message(true, 'test_prefix', $1)" {
				t.Errorf("Incorrect SQL statement: %s", sql)
			}

			// Verify the argument is a valid JSON representation of our event
			jsonStr, ok := args[0].(string)
			if !ok {
				t.Errorf("Expected args[0] to be a string, got %T", args[0])
			}

			var decodedEvent common.OutboxEvent
			if err := json.Unmarshal([]byte(jsonStr), &decodedEvent); err != nil {
				t.Errorf("Failed to unmarshal event: %v", err)
			}

			// Verify event fields
			if decodedEvent.AggregateType != "user" {
				t.Errorf("Expected AggregateType 'user', got '%s'", decodedEvent.AggregateType)
			}
			if decodedEvent.AggregateID != "123" {
				t.Errorf("Expected AggregateID '123', got '%s'", decodedEvent.AggregateID)
			}
			if decodedEvent.EventType != "user.created" {
				t.Errorf("Expected EventType 'user.created', got '%s'", decodedEvent.EventType)
			}

			// Ensure ID and timestamps are automatically populated
			if decodedEvent.ID == "" {
				t.Error("Expected ID to be populated")
			}
			if decodedEvent.CreatedAt.IsZero() {
				t.Error("Expected CreatedAt to be populated")
			}

			return 1, nil
		},
	}

	// Test the ExecOutboxEvent method
	if err := pgxTxAdapter.ExecOutboxEvent(context.Background(), event); err != nil {
		t.Errorf("ExecOutboxEvent() error = %v", err)
	}
}

// Mock implementations for testing

type mockPgxTx struct {
	execFunc func(ctx context.Context, sql string, args ...interface{}) (int64, error)
}

func (m *mockPgxTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return nil, nil
}

func (m *mockPgxTx) Commit(ctx context.Context) error {
	return nil
}

func (m *mockPgxTx) Rollback(ctx context.Context) error {
	return nil
}

func (m *mockPgxTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

func (m *mockPgxTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return nil
}

func (m *mockPgxTx) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (m *mockPgxTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

func (m *mockPgxTx) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	rowsAffected, err := m.execFunc(ctx, sql, args...)
	return pgconn.NewCommandTag(fmt.Sprintf("INSERT %d", rowsAffected)), err
}

func (m *mockPgxTx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return nil, nil
}

func (m *mockPgxTx) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return nil
}

func (m *mockPgxTx) Conn() *pgx.Conn {
	return nil
}
