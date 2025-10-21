# FactLib: Reliable Event Streaming with PostgreSQL WAL and the Outbox Pattern

## The Problem with Naive Event-Driven Architecture

Building reliable event-driven systems is harder than it seems. Consider this common scenario:

```go
// This is NOT reliable!
func CreateUser(user User) error {
    // 1. Save to database
    if err := db.Save(user); err != nil {
        return err
    }

    // 2. Emit event to Kafka
    if err := kafka.Produce("user.created", user); err != nil {
        // Oops! Data is saved but event failed
        // Now our system is in an inconsistent state
        return err
    }

    return nil
}
```

What happens if:
- The Kafka produce fails after the database commit?
- The server crashes between the database save and Kafka produce?
- Network partitions occur during event emission?

You end up with data in your database but no corresponding events in your message broker. Your system becomes inconsistent, and downstream services miss critical updates.

## Enter the Outbox Pattern

The outbox pattern solves this by making event emission atomic with your business transaction:

1. **Single Transaction**: Save your business data AND the outbox event in the same database transaction
2. **Separate Process**: A background worker reads outbox events and publishes them to your message broker
3. **At-Least-Once Delivery**: Events are guaranteed to be published, even if retries are needed

## FactLib: PostgreSQL WAL-Based Event Streaming

FactLib implements the outbox pattern using PostgreSQL's Write-Ahead Log (WAL) for maximum reliability and performance. Instead of polling database tables, we tap directly into PostgreSQL's logical replication stream.

### Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────┐
│  Service A      │    │    PostgreSQL    │    │   Kafka     │
│  (Producer)     │    │                  │    │             │
├─────────────────┤    ├──────────────────┤    ├─────────────┤
│ 1. Business TX  │───▶│ 2. WAL Event     │───▶│ 4. Message  │
│ 2. Emit Fact    │    │ 3. OwlPost       │    │             │
└─────────────────┘    │    Consumer      │    └─────────────┘
                       └──────────────────┘
```

### Key Components

#### 1. **Fact Emission** (`pkg/outbox/producer/`)
Emit events atomically within your business transactions:

```go
func CreateUser(ctx context.Context, tx pgx.Tx, user User) error {
    // Business logic
    if err := insertUser(ctx, tx, user); err != nil {
        return err
    }

    // Emit fact atomically
    fact, _ := common.NewFact("user", user.ID, "user.created", userData, metadata)
    if _, err := producer.Emit(ctx, fact); err != nil {
        return err // Entire transaction rolls back
    }

    return nil // Both data and event are committed together
}
```

#### 2. **WAL Subscriber** (`pkg/postgres/wal.go`)
Consumes PostgreSQL logical replication messages in real-time:

```go
// Subscribe to WAL events with custom prefix filtering
walSubscriber, _ := postgres.NewWALSubscriber(ctx, walConfig, logger)
events, _ := walSubscriber.Subscribe(ctx)

for event := range events {
    // Process each WAL event
    fmt.Printf("Received: %s.%s\n", event.Outbox.AggregateType, event.Outbox.EventType)
}
```

#### 3. **OwlPost Consumer** (`cmd/owlpost/`)
The main worker service that bridges PostgreSQL WAL to Kafka:

```go
// Set up consumer with Kafka handler
outboxConsumer, _ := consumer.NewOutboxConsumer(ctx, config, logger, postgresAdapter)
outboxConsumer.RegisterHandler(prefix, consumer.KafkaEventHandler(kafkaAdapter, logger))

// Start consuming and producing
outboxConsumer.Start(ctx)
```

#### 4. **Kafka Integration** (`pkg/outbox/consumer/kafka.go`)
Reliable Kafka production with acknowledgment tracking:

```go
// Produces to Kafka with proper partitioning and headers
topic := fmt.Sprintf("%s.%s", event.OutboxPrefix, event.Outbox.AggregateType)
kafkaAdapter.Produce(ctx, topic, key, value, headers)
```

## Setup and Configuration

### 1. PostgreSQL Configuration
Enable logical replication in your PostgreSQL instance:

```sql
-- Set in postgresql.conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

### 2. Database Setup
Run the initialization script (`scripts/init-postgres.sh`):

```bash
# Creates publication and replication slot
CREATE PUBLICATION factlib_publication FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('factlib_replication_slot', 'pgoutput');
```

### 3. Service Configuration
Configure OwlPost via environment variables (`cmd/owlpost/utils/config.go`):

```bash
SRC_SERVICE_NAME=user-service
WAL_PREFIX=user-service
MASTER_DATABASE_URL=postgres://user:pass@host:port/dbname
KAFKA_BROKERS=localhost:9092
REPLICATION_SLOT_NAME=factlib_slot
PUBLICATION_NAME=factlib_publication
```

### 4. Deploy with Docker
Use the provided Dockerfile.owlpost and docker-compose.yaml:

```bash
docker-compose up -d postgres kafka
docker-compose up owlpost
```

## Development and Testing

### Running Integration Tests
Test the complete WAL pipeline (`pkg/postgres/wal_integration_test.go`):

```bash
# Requires PostgreSQL with logical replication
go test -tags=integration ./pkg/postgres -run TestWALSubscriberIntegration
```

### End-to-End Example
See main.go for a complete working example.

### Build and Development
Uses justfile for common tasks:

```bash
just build-owlpost  # Build the consumer service
just test          # Run tests
just lint          # Run linting
```

## Reliability Guarantees

### ✅ **Atomicity**
Events are emitted within the same transaction as business data changes. If the transaction fails, no event is produced.

### ✅ **Durability**
Events are persisted in PostgreSQL's WAL before being acknowledged, surviving server crashes.

### ✅ **At-Least-Once Delivery**
The WAL subscriber tracks LSN positions and can resume from the last processed position after restarts.

### ✅ **Ordering**
Events maintain causal ordering through LSN sequencing and proper Kafka partitioning.

### ✅ **Health Monitoring**
Built-in health checks (`cmd/owlpost/delivery/health/`) ensure the consumer is actively processing events.

## Key Benefits

1. **Zero Data Loss**: Atomic emission prevents orphaned database changes
2. **High Performance**: Direct WAL consumption is faster than table polling
3. **Low Latency**: Near real-time event processing
4. **Operational Simplicity**: No additional infrastructure beyond PostgreSQL and Kafka
5. **Transactional Safety**: Full ACID compliance for event emission

## When to Use FactLib

FactLib is ideal when you need:
- **Reliable event sourcing** from PostgreSQL applications
- **Microservices communication** with strong consistency guarantees
- **Event-driven architectures** without compromising on reliability
- **Legacy system integration** where you can't change existing database schemas

## Conclusion

Building reliable distributed systems requires careful attention to failure modes. FactLib provides a battle-tested implementation of the outbox pattern using PostgreSQL's robust WAL mechanism, ensuring your events are delivered reliably without sacrificing performance or operational complexity.

The combination of PostgreSQL's ACID guarantees with Kafka's scalable messaging creates a solid foundation for event-driven architectures that you can actually rely on in production.

---

*For more details, explore the codebase or check out the integration tests to see FactLib in action.*