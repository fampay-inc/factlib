# Outbox Pattern Implementation

This package implements the outbox pattern for reliable event publishing in distributed systems. It follows hexagonal architecture principles and SOLID design patterns.

## Components

The outbox pattern implementation consists of two main components:

1. **Client**: Used by services to emit outbox events to PostgreSQL
2. **Service**: Used by service owners to read outbox events and publish them to Kafka

## Client Usage

The client part allows services to emit outbox events into PostgreSQL using the WAL (Write-Ahead Log) mechanism. It supports both JSON and Protobuf serialization formats.

```go
import (
    "context"
    "github.com/jackc/pgx/v5"
    "git.famapp.in/fampay-inc/factlib/pkg/logger"
    "git.famapp.in/fampay-inc/factlib/pkg/outbox/client"
)

func main() {
    ctx := context.Background()
    log := logger.NewLogger()
    
    // Connect to PostgreSQL
    conn, err := pgx.Connect(ctx, "postgres://user:password@localhost:5432/database")
    if err != nil {
        log.Error("failed to connect to database", "error", err)
        return
    }
    defer conn.Close(ctx)
    
    // Create outbox client
    outboxClient, err := client.NewPostgresAdapter(conn, log)
    if err != nil {
        log.Error("failed to create outbox client", "error", err)
        return
    }
    
    // Optionally set a custom prefix for logical decoding messages
    outboxClient = outboxClient.WithPrefix("my_service_outbox")
    
    // Emit a JSON event
    payload := []byte(`{"key": "value"}`)
    metadata := map[string]string{
        "source": "my-service",
        "version": "1.0.0",
    }
    
    eventID, err := outboxClient.EmitEvent(
        ctx,
        "user",             // aggregate type
        "123",              // aggregate ID
        "user_created",     // event type
        payload,            // event payload
        metadata,           // event metadata
    )
    if err != nil {
        log.Error("failed to emit event", "error", err)
        return
    }
    
    log.Info("emitted event", "id", eventID)
    
    // Emit a Protobuf event (requires proto definition)
    protoPayload := []byte{} // serialized protobuf message
    
    protoEventID, err := outboxClient.EmitProtoEvent(
        ctx,
        "user",             // aggregate type
        "123",              // aggregate ID
        "user_created",     // event type
        protoPayload,       // event payload
        metadata,           // event metadata
    )
    if err != nil {
        log.Error("failed to emit proto event", "error", err)
        return
    }
    
    log.Info("emitted proto event", "id", protoEventID)
}
```

## Service Usage

The service part reads outbox events from PostgreSQL WAL and publishes them to Kafka. Documentation for this component will be added as implementation progresses.

## Architecture

This implementation follows the hexagonal architecture pattern:

- **Domain**: Core business logic and domain models
- **Ports**: Interfaces that define how the domain interacts with the outside world
- **Adapters**: Implementations of the ports that connect to external systems

## Testing

The package includes comprehensive unit tests for all components. To run the tests:

```bash
go test -v ./pkg/outbox/...
```
