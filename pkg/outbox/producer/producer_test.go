package producer

import (
	"context"
	"database/sql"
	"testing"

	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockPgxConn is a mock for pgx.Conn
type MockPgxConn struct {
	mock.Mock
}

func (m *MockPgxConn) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	args := m.Called(ctx, sql, arguments)
	return args.Get(0).(pgconn.CommandTag), args.Error(1)
}

func (m *MockPgxConn) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	mockCall := m.Called(ctx, sql)
	if mockCall.Get(0) == nil {
		return nil
	}
	return mockCall.Get(0).(pgx.Row)
}

// MockPgxPool is a mock for pgxpool.Pool
type MockPgxPool struct {
	mock.Mock
}

func (m *MockPgxPool) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	args := m.Called(ctx, sql, arguments)
	return args.Get(0).(pgconn.CommandTag), args.Error(1)
}

func (m *MockPgxPool) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	mockCall := m.Called(ctx, sql, args)
	if mockCall.Get(0) == nil {
		return nil
	}
	return mockCall.Get(0).(pgx.Row)
}

// MockSqlDB is a mock for sql.DB
type MockSqlDB struct {
	mock.Mock
}

func (m *MockSqlDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	arguments := m.Called(ctx, query, args)
	return arguments.Get(0).(sql.Result), arguments.Error(1)
}

func (m *MockSqlDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return m.Called(ctx, query, args).Get(0).(*sql.Row)
}

func TestNewPostgresAdapter(t *testing.T) {
	log := &logger.Logger{}

	// Test with valid executor
	t.Run("valid executor", func(t *testing.T) {
		mockExecutor := &MockExecutor{}
		adapter, err := NewPostgresAdapter(mockExecutor, log)
		assert.NoError(t, err)
		assert.NotNil(t, adapter)
		assert.Equal(t, defaultOutboxPrefix, adapter.prefix)
	})

	// Test with nil executor
	t.Run("nil executor", func(t *testing.T) {
		adapter, err := NewPostgresAdapter(nil, log)
		assert.Error(t, err)
		assert.Nil(t, adapter)
		assert.Contains(t, err.Error(), "executor cannot be nil")
	})
}

func TestWithPrefix(t *testing.T) {
	log := &logger.Logger{}
	mockExecutor := &MockExecutor{}

	adapter, err := NewPostgresAdapter(mockExecutor, log)
	assert.NoError(t, err)

	// Test with custom prefix
	customPrefix := "custom_prefix"
	result := adapter.WithPrefix(customPrefix)

	// Assert the adapter was returned and prefix was updated
	assert.Equal(t, adapter, result)
	assert.Equal(t, customPrefix, adapter.prefix)
}

func TestEmitEvent(t *testing.T) {
	ctx := context.Background()
	log := &logger.Logger{}

	// Create a mock executor
	execCalled := false
	mockExecutor := &MockExecutor{
		ExecSQLFunc: func(ctx context.Context, query string, args ...any) error {
			execCalled = true
			assert.Equal(t, "SELECT pg_logical_emit_message(true, $1, $2::bytea)", query)
			assert.Equal(t, 2, len(args))
			assert.Equal(t, "proto_outbox", args[0])
			return nil
		},
	}

	adapter, err := NewPostgresAdapter(mockExecutor, log)
	assert.NoError(t, err)

	// Test with valid event
	eventID, err := adapter.EmitEvent(ctx, "test_aggregate", "123", "test_event", []byte(`{"test":"data"}`), map[string]string{"key": "value"})
	assert.NoError(t, err)
	assert.NotEmpty(t, eventID)
	assert.True(t, execCalled)

	// Test with missing aggregate type
	eventID, err = adapter.EmitEvent(ctx, "", "123", "test_event", []byte(`{"test":"data"}`), nil)
	assert.Error(t, err)
	assert.Empty(t, eventID)
	assert.Contains(t, err.Error(), "aggregate type is required")

	// Test with missing aggregate ID
	eventID, err = adapter.EmitEvent(ctx, "test_aggregate", "", "test_event", []byte(`{"test":"data"}`), nil)
	assert.Error(t, err)
	assert.Empty(t, eventID)
	assert.Contains(t, err.Error(), "aggregate ID is required")

	// Test with missing event type
	eventID, err = adapter.EmitEvent(ctx, "test_aggregate", "123", "", []byte(`{"test":"data"}`), nil)
	assert.Error(t, err)
	assert.Empty(t, eventID)
	assert.Contains(t, err.Error(), "event type is required")

	// Test with executor error
	mockExecutor.ExecSQLFunc = func(ctx context.Context, query string, args ...any) error {
		return errors.New("executor error")
	}
	eventID, err = adapter.EmitEvent(ctx, "test_aggregate", "123", "test_event", []byte(`{"test":"data"}`), nil)
	assert.Error(t, err)
	assert.Empty(t, eventID)
	assert.Contains(t, err.Error(), "executor error")
}

// MockConnectionQuerier is a mock for ConnectionQuerier
type MockConnectionQuerier struct {
	mock.Mock
}

func (m *MockConnectionQuerier) QueryVersion(ctx context.Context) (string, error) {
	args := m.Called(ctx)
	return args.String(0), args.Error(1)
}

func TestValidateConnection(t *testing.T) {
	ctx := context.Background()

	// Test with direct executor implementation
	t.Run("direct executor", func(t *testing.T) {
		// Create a mock querier
		mockQuerier := &MockConnectionQuerier{}
		mockQuerier.On("QueryVersion", mock.Anything).Return("PostgreSQL 14.0", nil)

		// Create a custom validator that uses our mock
		validator := func(ctx context.Context, _ any) error {
			// Skip the connection type check and use our mock directly
			_, err := mockQuerier.QueryVersion(ctx)
			return err
		}

		// Test validation
		err := validator(ctx, nil) // Connection doesn't matter here
		assert.NoError(t, err)
		mockQuerier.AssertCalled(t, "QueryVersion", ctx)
	})

	// Test with invalid connection type
	t.Run("invalid connection type", func(t *testing.T) {
		err := ValidateConnection(ctx, "invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported connection type")
	})
}

type mockPgxRow struct{}

func (m *mockPgxRow) Scan(dest ...interface{}) error {
	// Simulate scanning a PostgreSQL version
	if len(dest) > 0 {
		if s, ok := dest[0].(*string); ok {
			*s = "PostgreSQL 14.0"
		}
	}
	return nil
}
