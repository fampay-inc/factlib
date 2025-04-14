package client_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"git.famapp.in/fampay-inc/factlib/pkg/client"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
)

// TestEvent is a sample event structure that would be used by a service
type TestEvent struct {
	ID      string    `json:"id"`
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
}

// This test requires a running PostgreSQL instance
// It can be run with:
// go test -tags=integration ./pkg/client -run TestClientIntegration
func TestClientIntegration(t *testing.T) {
	// Skip if not running integration tests
	if os.Getenv("INTEGRATION_TEST") != "true" {
		t.Skip("Skipping integration test. Set INTEGRATION_TEST=true to run")
	}

	// Configure logging
	loggerConfig := logger.Config{
		Level:      "debug",
		JSONOutput: false,
		WithCaller: true,
	}
	logr := logger.New(loggerConfig)
	logr = logr.With("test", "client_integration")

	// Create context
	ctx := context.Background()

	// Get PostgreSQL connection string from environment or use default
	pgConnString := os.Getenv("PG_CONN_STRING")
	if pgConnString == "" {
		pgConnString = "postgres://postgres:postgres@localhost:6432/outbox_example?sslmode=disable"
	}

	// Initialize the client
	outboxClient, err := client.NewPostgresClient(ctx, client.PostgresConfig{
		ConnectionString: pgConnString,
	}, logr)
	require.NoError(t, err, "Failed to create PostgreSQL outbox client")

	// Create test event data
	eventID := uuid.New().String()
	testEvent := TestEvent{
		ID:      eventID,
		Message: "Integration test event",
		Time:    time.Now(),
	}

	// Marshal the test event to JSON
	payload, err := json.Marshal(testEvent)
	require.NoError(t, err, "Failed to marshal test event")

	// Define metadata
	metadata := map[string]string{
		"source": "integration-test",
		"env":    "test",
	}

	// Publish the event using the outbox client
	publishedID, err := outboxClient.Publish(
		ctx,
		"test",                // aggregate type
		eventID,              // aggregate ID
		"test-event-created", // event type
		payload,              // event payload
		metadata,             // metadata
	)
	require.NoError(t, err, "Failed to publish event")
	require.NotEmpty(t, publishedID, "Published event ID should not be empty")

	logr.Info("Successfully published event", 
		"event_id", publishedID,
		"aggregate_id", eventID)

	// Test additional adapter functionality if needed
	// For example, testing the WithConn adapter...
}

// TestSqlAdapter tests the SQL adapter integration
func TestSqlAdapter(t *testing.T) {
	// Skip if not running integration tests
	if os.Getenv("INTEGRATION_TEST") != "true" {
		t.Skip("Skipping integration test. Set INTEGRATION_TEST=true to run")
	}

	// Configure logging
	loggerConfig := logger.Config{
		Level:      "debug",
		JSONOutput: false,
		WithCaller: true,
	}
	logr := logger.New(loggerConfig)
	logr = logr.With("test", "sql_adapter")

	// Example of how a service would use the SQL adapter
	t.Run("SQLAdapter", func(t *testing.T) {
		// This would be replaced with actual SQL DB initialization in a real service
		logr.Info("SQL Adapter would be tested here with an actual sql.DB connection")
		// The test would then use client.WithSqlDB to create an adapter
	})
}

// TestPgxAdapter tests the pgx adapter integration
func TestPgxAdapter(t *testing.T) {
	// Skip if not running integration tests
	if os.Getenv("INTEGRATION_TEST") != "true" {
		t.Skip("Skipping integration test. Set INTEGRATION_TEST=true to run")
	}

	// Configure logging
	loggerConfig := logger.Config{
		Level:      "debug",
		JSONOutput: false,
		WithCaller: true,
	}
	logr := logger.New(loggerConfig)
	logr = logr.With("test", "pgx_adapter")

	// Example of how a service would use the pgx adapter
	t.Run("PgxAdapter", func(t *testing.T) {
		// This would be replaced with actual pgx initialization in a real service
		logr.Info("Pgx Adapter would be tested here with an actual pgx.Conn")
		// The test would then use client.WithPgxConn to create an adapter
	})
}
