# Factlib - Outbox Pattern Library for Go

Factlib is a robust Golang library for implementing the outbox pattern with PostgreSQL's Write-Ahead Log (WAL) and Kafka integration. It provides reliable event publishing through database transactions and leverages PostgreSQL's logical decoding to capture events efficiently without requiring a separate outbox table.

## Features

- 🔄 **WAL-Based Outbox Pattern**: Guarantees at-least-once delivery by emitting events directly to PostgreSQL's WAL
- 📊 **No Outbox Table Required**: Uses `pg_logical_emit_message` to write events directly to WAL
- 🚀 **Kafka Integration**: Uses franz-go library for high-performance Kafka production
- 🔄 **Flexible Payload Format**: Supports both JSON and Protocol Buffers for message payloads
- 🔍 **Zerolog Logging**: Structured logging with key=value format
- 🔧 **Hexagonal Architecture**: Clean, SOLID design with separate client and service components
- 🔒 **Transactional Guarantees**: Ensures atomicity between database operations and event publishing

## Architecture

### Components

1. **Client**: Used by services to emit messages to PostgreSQL WAL
2. **Worker**: Reads events from WAL and publishes them to Kafka
3. **Logger**: Structured logging with zerolog
4. **Postgres**: Interfaces with PostgreSQL for WAL reading/writing
5. **Kafka**: Produces messages to Kafka topics

### Flow

1. Service uses the client to emit an event directly to PostgreSQL's WAL using `pg_logical_emit_message`
2. Event is encoded with base64 to ensure proper handling of binary data through PostgreSQL's logical replication
3. Service subscribes to WAL events via logical replication slots
4. Service processes events and publishes them to Kafka based on registered handlers
5. Consumers read from Kafka topics

## Prerequisites

- PostgreSQL 10+ with logical replication enabled
- Kafka cluster (for production use)
- Go 1.18+
- GORM (optional, for ORM integration)

## PostgreSQL Configuration

Ensure your PostgreSQL server has logical replication enabled:

```
# In postgresql.conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10

# Create a publication for the outbox_events table
CREATE PUBLICATION factlib_publication FOR ALL TABLES;

# Create a replication slot
SELECT pg_create_logical_replication_slot('factlib_slot', 'pgoutput');
SELECT pg_create_logical_replication_slot('factlib_replication_slot', 'pgoutput');
```

## Usage

### Client Integration

```go
// Create a PostgreSQL connection
sqlDB, err := sql.Open("pgx", "host=localhost user=postgres password=postgres dbname=mydb port=5432 sslmode=disable")
if err != nil {
    log.Fatal().Err(err).Msg("failed to connect to database")
}

// Create a logger
logger := &logger.Logger{}

// Create the outbox adapter
outboxAdapter, err := client.NewPostgresAdapter(sqlDB, logger)
if err != nil {
    log.Fatal().Err(err).Msg("failed to create outbox adapter")
}

// Optionally set a custom prefix for the outbox events
outboxAdapter = outboxAdapter.WithPrefix("my_service").(*client.PostgresAdapter)

// Example of emitting an event within a transaction
tx, err := sqlDB.Begin()
if err != nil {
    return err
}
defer tx.Rollback()

// Create your domain event
event := client.Event{
    AggregateID:   "user-123",
    AggregateType: "user",
    EventType:     "user_created",
    Payload:       []byte(`{"id":"user-123","name":"John Doe","email":"john@example.com"}`),
}

// Emit the event using the transaction
if err := outboxAdapter.EmitWithTx(context.Background(), tx, event); err != nil {
    return fmt.Errorf("failed to emit event: %w", err)
}

// Commit the transaction
if err := tx.Commit(); err != nil {
    return fmt.Errorf("failed to commit transaction: %w", err)
}
```

### Integration with GORM

When using GORM, you'll need to extract the underlying database connection:

```go
// Create GORM DB connection
db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
if err != nil {
    log.Fatal().Err(err).Msg("failed to connect to database")
}

// Get the underlying SQL DB connection for the outbox adapter
sqlDB, err := db.DB()
if err != nil {
    log.Fatal().Err(err).Msg("failed to get underlying DB connection")
}

// Create the outbox adapter with the SQL connection
outboxAdapter, err := client.NewPostgresAdapter(sqlDB, logger)
if err != nil {
    log.Fatal().Err(err).Msg("failed to create outbox adapter")
}

// Example of using the outbox pattern with GORM transactions
err = db.Transaction(func(tx *gorm.DB) error {
    // Perform database operations
    user := User{ID: uuid.New(), Name: "Jane Smith", Email: "jane@example.com"}
    if err := tx.Create(&user).Error; err != nil {
        return err
    }
    
    // Get the underlying transaction for the outbox adapter
    sqlTx, err := tx.DB().BeginTx(context.Background(), nil)
    if err != nil {
        return err
    }
    
    // Create and emit the event
    event := client.Event{
        AggregateID:   user.ID.String(),
        AggregateType: "user",
        EventType:     "user_created",
        Payload:       userToJSON(user),
    }
    
    if err := outboxAdapter.EmitWithTx(context.Background(), sqlTx, event); err != nil {
        return err
    }
    
    return nil
})
```

## Advantages of WAL-Based Outbox Pattern

Factlib implements the outbox pattern using PostgreSQL's Write-Ahead Log (WAL) instead of the traditional table-based approach, offering several advantages:

1. **Performance**: Emitting events directly to the WAL is more efficient than writing to a separate outbox table
2. **No Cleanup Required**: Traditional outbox patterns require a separate process to clean up processed events, which is not needed with the WAL approach
3. **Reduced Database Load**: No additional queries to poll for new events or delete processed ones
4. **Real-time Processing**: Events are captured in real-time through PostgreSQL's logical replication
5. **Transactional Integrity**: Events are only emitted if the database transaction commits

## Testing

Factlib is designed to be easily testable. Here's an example of how to test code that uses the outbox pattern:

```go
func TestUserService_CreateUser(t *testing.T) {
	// Setup test database
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Failed to create mock DB: %v", err)
	}
	defer db.Close()

	// Create mock logger
	logger := &logger.Logger{}

	// Create outbox adapter with the mock DB
	outboxAdapter, err := client.NewPostgresAdapter(db, logger)
	if err != nil {
		t.Fatalf("Failed to create outbox adapter: %v", err)
	}

	// Setup expectations
	mock.ExpectBegin()
	// Expect database operations
	mock.ExpectExec("INSERT INTO users").WillReturnResult(sqlmock.NewResult(1, 1))
	// Expect pg_logical_emit_message call for the outbox event
	mock.ExpectExec("SELECT pg_logical_emit_message").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	// Create service with mock dependencies
	userService := NewUserService(db, outboxAdapter, logger)

	// Execute the method under test
	user := User{ID: "123", Name: "Test User", Email: "test@example.com"}
	err = userService.CreateUser(context.Background(), user)

	// Verify expectations
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}
```

## Installation

```bash
go get git.famapp.in/fampay-inc/factlib
```

## Usage

### Client Usage

```go
import (
    "context"
    "log"
    
    "git.famapp.in/fampay-inc/factlib/pkg/client"
    "git.famapp.in/fampay-inc/factlib/pkg/logger"
)

func main() {
    // Initialize logger
    log := logger.New(logger.Config{
        Level:      "info",
        JSONOutput: true,
        WithCaller: true,
    })

    // Initialize client
    postgresClient, err := client.NewPostgresClient(context.Background(), client.PostgresConfig{
        ConnectionString: "postgres://user:password@localhost:5432/dbname?sslmode=disable",
    }, log)
    if err != nil {
        log.Fatal("failed to create client", err)
    }
    defer postgresClient.Close()

    // Publish an event
    eventID, err := postgresClient.Publish(
        context.Background(),
        "user",                 // Aggregate type
        "user123",              // Aggregate ID
        "user.created",         // Event type
        []byte(`{"id":"123"}`), // Payload (could be protobuf)
        map[string]string{      // Metadata
            "source": "api",
        },
    )
    if err != nil {
        log.Fatal("failed to publish event", err)
    }
    
    log.Info("published event", "id", eventID)
}
```

### Worker Usage

```go
import (
    "context"
    "log"
    "time"
    
    "git.famapp.in/fampay-inc/factlib/pkg/kafka"
    "git.famapp.in/fampay-inc/factlib/pkg/logger"
    "git.famapp.in/fampay-inc/factlib/pkg/postgres"
    "git.famapp.in/fampay-inc/factlib/pkg/worker"
)

func main() {
    // Initialize logger
    log := logger.New(logger.Config{
        Level:      "info",
        JSONOutput: true,
        WithCaller: true,
    })

    // Initialize WAL subscriber
    walSubscriber, err := postgres.NewWALSubscriber(postgres.WALConfig{
        Host:     "localhost",
        Port:     5432,
        User:     "postgres",
        Password: "password",
        Database: "dbname",
    }, log)
    if err != nil {
        log.Fatal("failed to create WAL subscriber", err)
    }

    // Initialize Kafka producer
    kafkaProducer, err := kafka.NewProducer(kafka.ProducerConfig{
        Brokers:  []string{"localhost:9092"},
        ClientID: "outbox-worker",
    }, log)
    if err != nil {
        log.Fatal("failed to create Kafka producer", err)
    }

    // Initialize worker
    outboxWorker := worker.NewWorker(walSubscriber, kafkaProducer, log, worker.Config{
        PollingInterval: 100 * time.Millisecond,
    })

    // Register handlers for different aggregate types
    err = outboxWorker.Register(
        "user",                       // Aggregate type
        "user-events",                // Kafka topic
        worker.NewExampleHandler(log), // Handler
    )
    if err != nil {
        log.Fatal("failed to register handler", err)
    }

    // Start the worker
    ctx := context.Background()
    if err := outboxWorker.Start(ctx); err != nil {
        log.Fatal("failed to start worker", err)
    }
    defer outboxWorker.Close()

    // Wait for termination signal
    // ...
}
```

## Custom Event Handlers

You can create custom event handlers by implementing the `common.EventHandler` interface:

```go
type MyCustomHandler struct {
    logger *logger.Logger
}

func NewMyCustomHandler(log *logger.Logger) *MyCustomHandler {
    return &MyCustomHandler{
        logger: log,
    }
}

func (h *MyCustomHandler) Handle(ctx context.Context, event common.OutboxEvent) error {
    // Handle the event
    h.logger.Info("handling event", 
        "id", event.ID, 
        "aggregate_type", event.AggregateType, 
        "event_type", event.EventType)
    
    // Do something with the event payload
    // e.g., unmarshal protobuf message
    
    return nil
}
```

## Protobuf Integration

The library supports gRPC/protobuf for message serialization. You can define your protobuf messages and use them as the payload.

Example:

```go
// Define protobuf message in .proto file
message UserCreated {
    string user_id = 1;
    string name = 2;
    string email = 3;
}

// In your application
func publishUserCreated(client common.OutboxClient, user User) error {
    // Serialize protobuf message
    userCreated := &pb.UserCreated{
        UserId: user.ID,
        Name:   user.Name,
        Email:  user.Email,
    }
    
    payload, err := proto.Marshal(userCreated)
    if err != nil {
        return err
    }
    
    // Publish event
    _, err = client.Publish(
        context.Background(),
        "user",
        user.ID,
        "user.created",
        payload,
        nil,
    )
    
    return err
}
```

## License

MIT
