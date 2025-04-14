package postgres_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
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
		Host:     "localhost",
		Port:     6432,
		User:     "postgres",
		Password: "postgres",
		Database: "outbox_example",
		ReplicationSlotName: "factlib_test_slot",
		PublicationName:     "factlib_pub",
	}

	// Create a WAL subscriber instance
	walSubscriber, err := postgres.NewWALSubscriber(walConfig, logr)
	require.NoError(t, err, "Failed to create WAL subscriber")

	// Create a channel and wait group to synchronize the test
	messageReceived := make(chan common.OutboxEvent)
	var wg sync.WaitGroup
	wg.Add(1)

	// We'll process events directly from the subscription channel rather than setting a handler

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

	// Publish a test event
	eventID := uuid.New().String()
	payload := []byte(`{"test":"data"}`)
	metadata := map[string]string{"source": "integration_test"}

	publishedID, err := outboxClient.Publish(
		ctx,
		"integration_test",  // aggregate type
		eventID,            // aggregate ID
		"test.event",       // event type
		payload,            // payload
		metadata,           // metadata
	)
	require.NoError(t, err, "Failed to publish test event")
	require.NotEmpty(t, publishedID, "Published event ID should not be empty")

	logr.Info("Test event published, waiting for it to be received by the WAL subscriber",
		"event_id", publishedID)

	// Wait for the message to be received or timeout
	var receivedEvent common.OutboxEvent
	select {
	case receivedEvent = <-messageReceived:
		// Message received successfully
	case <-ctx.Done():
		t.Fatal("Timed out waiting for message to be received")
	}

	// Verify the received event matches what we sent
	assert.Equal(t, publishedID, receivedEvent.ID, "Event ID should match")
	assert.Equal(t, "integration_test", receivedEvent.AggregateType, "Aggregate type should match")
	assert.Equal(t, eventID, receivedEvent.AggregateID, "Aggregate ID should match")
	assert.Equal(t, "test.event", receivedEvent.EventType, "Event type should match")
	assert.Equal(t, string(payload), string(receivedEvent.Payload), "Payload should match")
	assert.Equal(t, metadata["source"], receivedEvent.Metadata["source"], "Metadata should match")

	// Cancel the context to stop the subscriber
	cancel()

	// Wait for the WAL subscriber to shut down gracefully
	wg.Wait()
}
