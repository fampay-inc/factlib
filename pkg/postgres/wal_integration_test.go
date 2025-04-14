package postgres_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"git.famapp.in/fampay-inc/factlib/pkg/client"
	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"git.famapp.in/fampay-inc/factlib/pkg/postgres"
)

// This test requires a running PostgreSQL instance with logical replication configured
// It can be run with:
// go test -tags=integration ./pkg/postgres -run TestWALSubscriberIntegration
func TestWALSubscriberIntegration(t *testing.T) {
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
	logr = logr.With("test", "wal_subscriber_integration")

	// Create context with timeout for the test
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get PostgreSQL connection string from environment or use default
	pgConnString := os.Getenv("PG_CONN_STRING")
	if pgConnString == "" {
		pgConnString = "postgres://postgres:postgres@localhost:6432/outbox_example?sslmode=disable"
	}

	// Initialize the PostgreSQL client for emitting messages
	outboxClient, err := client.NewPostgresClient(ctx, client.PostgresConfig{
		ConnectionString: pgConnString,
	}, logr)
	require.NoError(t, err, "Failed to create PostgreSQL outbox client")

	// Setup WAL subscriber configuration
	walConfig := postgres.WALConfig{
		Host:                "localhost",
		Port:                6432,
		User:                "postgres",
		Password:            "postgres",
		Database:            "outbox_example",
		ReplicationSlotName: "factlib_test_slot",
		PublicationName:     "factlib_test_pub",
	}

	// Create a WAL subscriber instance
	walSubscriber, err := postgres.NewWALSubscriber(walConfig, logr)
	require.NoError(t, err, "Failed to create WAL subscriber")

	// Create a channel and wait group to synchronize the test
	messageReceived := make(chan common.OutboxEvent)
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
	time.Sleep(5 * time.Second)

	// Create a direct connection to PostgreSQL for querying the replication slot
	pgConfig, err := pgx.ParseConfig(pgConnString)
	if err != nil {
		t.Fatalf("Failed to parse connection string: %v", err)
	}
	
	pgConn, err := pgx.ConnectConfig(ctx, pgConfig)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pgConn.Close(ctx)
	
	// Create a test table for the publication
	_, err = pgConn.Exec(ctx, `CREATE TABLE IF NOT EXISTS test_events (
		id TEXT PRIMARY KEY,
		aggregate_type TEXT NOT NULL,
		aggregate_id TEXT NOT NULL,
		event_type TEXT NOT NULL,
		payload JSONB,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// First, check if the replication slot exists
	var slotExists bool
	err = pgConn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", 
		"factlib_test_slot").Scan(&slotExists)
	if err != nil {
		t.Fatalf("Failed to check if replication slot exists: %v", err)
	}
	
	if !slotExists {
		// Create the replication slot if it doesn't exist
		_, err = pgConn.Exec(ctx, "SELECT pg_create_logical_replication_slot('factlib_test_slot', 'pgoutput')")
		if err != nil {
			t.Fatalf("Failed to create replication slot: %v", err)
		}
		logr.Info("Created replication slot", "name", "factlib_test_slot")
	}

	// Create a publication specifically for the test table
	_, err = pgConn.Exec(ctx, "DROP PUBLICATION IF EXISTS factlib_test_pub")
	if err != nil {
		t.Fatalf("Failed to drop existing publication: %v", err)
	}

	_, err = pgConn.Exec(ctx, "CREATE PUBLICATION factlib_test_pub FOR TABLE test_events")
	if err != nil {
		t.Fatalf("Failed to create publication: %v", err)
	}
	logr.Info("Created publication for test table", "name", "factlib_test_pub")

	// Publish a test event both via the client and directly to the test table
	eventID := uuid.New().String()
	payload := []byte(`{"test":"data"}`)
	metadata := map[string]string{"source": "integration_test"}

	// Publish via client (using pg_logical_emit_message)
	publishedID, err := outboxClient.Publish(
		ctx,
		"integration_test",  // aggregate type
		eventID,            // aggregate ID
		"test.event",       // event type
		payload,            // payload
		metadata,           // metadata
	)
	require.NoError(t, err, "Failed to publish test event via client")
	require.NotEmpty(t, publishedID, "Published event ID should not be empty")

	// Also insert directly into the test table to trigger WAL events
	_, err = pgConn.Exec(ctx, `
		INSERT INTO test_events (id, aggregate_type, aggregate_id, event_type, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, publishedID, "integration_test", eventID, "test.event", string(payload), time.Now())
	require.NoError(t, err, "Failed to insert test event into table")

	logr.Info("Test event published, waiting for it to be received by the WAL subscriber",
		"event_id", publishedID)
			

	// Wait for the message to be received or timeout
	select {
	case receivedEvent := <-messageReceived:
		// Message received successfully
		// Verify the received event matches what we sent
		assert.Equal(t, publishedID, receivedEvent.ID, "Event ID should match")
		assert.Equal(t, "integration_test", receivedEvent.AggregateType, "Aggregate type should match")
		assert.Equal(t, eventID, receivedEvent.AggregateID, "Aggregate ID should match")
		assert.Equal(t, "test.event", receivedEvent.EventType, "Event type should match")
		assert.Equal(t, string(payload), string(receivedEvent.Payload), "Payload should match")
		assert.Equal(t, metadata["source"], receivedEvent.Metadata["source"], "Metadata should match")
	case <-time.After(15 * time.Second):
		t.Fatal("Timed out waiting for message to be received")
	}

	// Cancel the context to stop the subscriber
	cancel()

	// Wait for the WAL subscriber to shut down gracefully
	wg.Wait()
}
