# Factlib - Outbox Pattern Library for Go

Factlib is a robust Golang library for implementing the outbox pattern with PostgreSQL WAL logs and Kafka integration. It provides reliable event publishing through database transactions and leverages PostgreSQL's logical decoding to capture events efficiently.

## Features

- 🔄 **Outbox Pattern Implementation**: Guarantees at-least-once delivery by storing events in the database before publishing to Kafka
- 📊 **PostgreSQL WAL Integration**: Uses logical decoding messages for Change Data Capture (CDC)
- 🚀 **Kafka Integration**: Uses franz-go library for high-performance Kafka production
- 🔄 **gRPC Message Format**: Supports structured message payloads with Protocol Buffers
- 🔍 **Zerolog Logging**: Structured logging with key=value format
- 🔧 **Separation of Concerns**: Clean architecture with separate client and worker components

## Architecture

### Components

1. **Client**: Used by services to emit messages to PostgreSQL WAL
2. **Worker**: Reads events from WAL and publishes them to Kafka
3. **Logger**: Structured logging with zerolog
4. **Postgres**: Interfaces with PostgreSQL for WAL reading/writing
5. **Kafka**: Produces messages to Kafka topics

### Flow

1. Service uses the client to publish an event to PostgreSQL
2. Event is stored in the `outbox_events` table and also emitted as a logical decoding message
3. Worker subscribes to WAL events via logical replication
4. Worker processes events and publishes them to Kafka based on registered handlers
5. Consumers read from Kafka topics

## Prerequisites

- PostgreSQL 10+ with logical replication enabled
- Kafka cluster
- Go 1.18+

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
SELECT pg_create_logical_replication_slot('factlib_replication_slot', 'pgoutput');
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
