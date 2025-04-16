package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"git.famapp.in/fampay-inc/factlib/pkg/outbox/producer"
	"git.famapp.in/fampay-inc/factlib/pkg/postgres"
)

// This test requires a running PostgreSQL instance with logical replication configured
// It can be run with:
// go test -tags=integration ./pkg/postgres -run TestWALSubscriberIntegration
//
// Note: The PostgreSQL server must have logical replication enabled (wal_level = logical)
// This typically requires admin privileges to set in postgresql.conf and restart the server
func TestWALSubscriberIntegration(t *testing.T) {
	// Skip the test if we're not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Define all configuration in one place
	config := struct {
		// PostgreSQL connection details
		DatabaseURL string

		// WAL configuration
		ReplicationSlotName string
		PublicationName     string
		OutboxPrefix        string

		// Test configuration
		Timeout          time.Duration
		EventWaitTimeout time.Duration
	}{
		// PostgreSQL connection details
		DatabaseURL: "postgres://postgres:postgres@127.0.0.1:6432/outbox_example",
		// WAL configuration
		ReplicationSlotName: "factlib_test_slot",
		PublicationName:     "factlib_test_pub",
		OutboxPrefix:        "proto_outbox",
		// Test configuration
		Timeout:          30 * time.Second,
		EventWaitTimeout: 30 * time.Second,
	}

	// Configure logging
	loggerConfig := logger.Config{
		Level:      "debug",
		WithCaller: true,
	}

	logr := logger.New(loggerConfig)
	logr = logr.With("test", "wal_subscriber_integration")

	// Create context with timeout for the test
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()

	// Initialize the PostgreSQL client for emitting messages
	pgConfig, err := pgx.ParseConfig(config.DatabaseURL)
	require.NoError(t, err, "Failed to parse connection string")

	pgConn, err := pgx.ConnectConfig(ctx, pgConfig)
	require.NoError(t, err, "Failed to connect to database")
	defer pgConn.Close(ctx)

	// Check if logical replication is enabled
	var walLevel string
	err = pgConn.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel)
	require.NoError(t, err, "Failed to check wal_level")

	if walLevel != "logical" {
		t.Skip("Skipping test: PostgreSQL server does not have logical replication enabled (wal_level != logical)")
	}

	logr.Info("PostgreSQL server has logical replication enabled", "wal_level", walLevel)

	// Create a pgx executor for the connection
	executor, err := producer.NewPgxExecutor(pgConn)
	require.NoError(t, err, "Failed to create pgx executor")

	// Create the adapter with the executor
	adapter, err := producer.NewPostgresAdapter(executor, logr)
	require.NoError(t, err, "Failed to create PostgreSQL outbox producer")

	// Set a custom prefix for the test
	outboxProducer := adapter.WithPrefix(config.OutboxPrefix)

	// Check if the replication slot exists and drop it if it's active
	var slotExists bool
	err = pgConn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		config.ReplicationSlotName).Scan(&slotExists)
	require.NoError(t, err, "Failed to check if replication slot exists")

	if slotExists {
		logr.Info("Dropping existing replication slot", "slot_name", config.ReplicationSlotName)
		_, err = pgConn.Exec(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", config.ReplicationSlotName))
		if err != nil {
			logr.Warn("Failed to drop replication slot, it might be in use", "error", err.Error())
			// Continue with the unique slot name instead of failing the test
		}
	}

	// Setup WAL subscriber configuration with the unique names
	walConfig := postgres.WALConfig{
		DatabaseURL:         config.DatabaseURL,
		ReplicationSlotName: config.ReplicationSlotName,
		PublicationName:     config.PublicationName,
		OutboxPrefix:        config.OutboxPrefix,
	}

	// Create a WAL subscriber instance
	walSubscriber, err := postgres.NewWALSubscriber(walConfig, logr)
	require.NoError(t, err, "Failed to create WAL subscriber")

	// Create a channel and wait group to synchronize the test
	messageReceived := make(chan *postgres.Event)
	var wg sync.WaitGroup
	wg.Add(1)

	// Subscribe to WAL events
	eventChan, err := walSubscriber.Subscribe(ctx)
	require.NoError(t, err, "Failed to subscribe to WAL events")

	// Start the WAL message processor in a goroutine
	go func() {
		defer wg.Done()
		for {
			select {
			case event, ok := <-eventChan:
				if !ok {
					// Channel closed
					return
				}
				// Forward the event to our test channel
				select {
				case messageReceived <- event:
					// Message forwarded
				case <-ctx.Done():
					// Context cancelled
					return
				}
			case <-ctx.Done():
				// Context cancelled
				return
			}
		}
	}()

	// Wait a moment for the WAL subscriber to establish connection
	time.Sleep(2 * time.Second)

	// Publish a test event both via the client and directly to the test table
	eventID := uuid.New().String()
	// Use a simple string as payload to avoid encoding issues
	payload := []byte("test data")
	metadata := map[string]string{"source": "integration_test"}

	// Publish via producer (using pg_logical_emit_message)
	publishedID, err := outboxProducer.EmitEvent(
		ctx,
		"integration_test", // aggregate type
		eventID,            // aggregate ID
		"test.event",       // event type
		payload,            // payload
		metadata,           // metadata
	)
	require.NoError(t, err, "Failed to publish test event via client")
	require.NotEmpty(t, publishedID, "Published event ID should not be empty")
	// Sleep briefly to allow the WAL subscriber to process the event
	time.Sleep(1 * time.Second)

	logr.Info("Test event published, waiting for it to be received by the WAL subscriber",
		"event_id", publishedID)

	// Wait for the message to be received or timeout
	select {
	case pgEvent := <-messageReceived:
		// Message received successfully
		// Verify the received event matches what we sent
		receivedEvent := pgEvent.Outbox
		logr.Info("Received event", "event_id", receivedEvent.Id, "expected_id", publishedID)
		assert.Equal(t, publishedID, receivedEvent.Id, "Event ID should match")
		assert.Equal(t, "integration_test", receivedEvent.AggregateType, "Aggregate type should match")
		assert.Equal(t, eventID, receivedEvent.AggregateId, "Aggregate ID should match")
		assert.Equal(t, "test.event", receivedEvent.EventType, "Event type should match")
		assert.Contains(t, string(receivedEvent.Payload), string(payload), "Payload should contain the expected data")
		assert.Equal(t, metadata["source"], receivedEvent.Metadata["source"], "Metadata should match")
	case <-time.After(config.EventWaitTimeout):
		// If we timed out, let's check the replication slot status
		_, err := walSubscriber.CheckReplicationSlot(ctx)
		if err != nil {
			logr.Error("Failed to check replication slot status", err, "error", err.Error())
		}
		t.Fatal("Timed out waiting for message to be received")
	}
	// Cancel the context to stop the subscriber
	cancel()

	// Wait for the WAL subscriber to shut down gracefully
	wg.Wait()
}
