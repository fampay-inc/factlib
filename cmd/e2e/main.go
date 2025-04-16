package main

import (
	"context"
	"flag"

	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"git.famapp.in/fampay-inc/factlib/pkg/outbox/consumer"
	"git.famapp.in/fampay-inc/factlib/pkg/outbox/producer"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type AppConfig struct {
	DatabaseURL         string
	ReplicationSlotName string
	PublicationName     string
	OutboxPrefix        string
}

var log *logger.Logger
var cfg AppConfig

func init() {
	loggerConfig := logger.Config{
		Level:      "debug",
		WithCaller: true,
	}
	log = logger.New(loggerConfig)

	cfg = AppConfig{
		DatabaseURL:         "postgres://postgres:postgres@localhost:6432/outbox_example",
		ReplicationSlotName: "outbox_slot",
		PublicationName:     "outbox_pub",
		OutboxPrefix:        "westeros_app",
	}
}

func main() {
	outboxService := flag.Bool("outbox", false, "Run outbox service")
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if *outboxService {
		if err := OutboxService(ctx); err != nil {
			log.Error("failed to start outbox service", err, "error", err.Error())
		}
	}
	pgConn, err := pgx.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("failed to connect to database", err, "error", err.Error())
		return
	}
	defer pgConn.Close(ctx)
	userData := []byte("user data")
	createUserWithEvents(ctx, pgConn, userData)
}

func OutboxService(ctx context.Context) error {
	// Set up PostgreSQL connection

	pgConn, err := pgx.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pgConn.Close(ctx)

	// Set up the outbox producer with transaction support
	// executor, err := producer.NewPgxExecutor(pgConn)
	// if err != nil {
	// 	return err
	// }

	// outboxProducer, err := producer.NewPostgresAdapter(executor, log)
	// if err != nil {
	// 	return err
	// }

	// Set up Kafka with exactly-once semantics
	kafkaConfig := consumer.KafkaConfig{
		BootstrapServers: []string{"127.0.0.1:9092"}, // Use localhost instead of container name
		ClientID:         "outbox-service",
		RequiredAcks:     -1, // All ISR acks
	}

	// Create Kafka adapter
	kafkaAdapter, err := consumer.NewKafkaAdapter(kafkaConfig, log)
	if err != nil {
		return err
	}
	defer kafkaAdapter.Close()

	// Set up outbox consumer with processing tracking
	consumerConfig := consumer.Config{
		ConnectionString: cfg.DatabaseURL,
		WalPrefix:        cfg.OutboxPrefix,
	}

	outboxConsumer, err := consumer.NewOutboxConsumer(ctx, consumerConfig, log)
	if err != nil {
		return err
	}
	defer outboxConsumer.Stop()

	// Register handler for user events
	outboxConsumer.RegisterHandler(cfg.OutboxPrefix, consumer.KafkaEventHandler(kafkaAdapter))

	// Start the consumer
	if err := outboxConsumer.Start(ctx); err != nil {
		return err
	}

	// Block until context is canceled
	<-ctx.Done()
	return nil
}

// Example of using the transactional adapter
func createUserWithEvents(ctx context.Context, pgConn *pgx.Conn, userData []byte) error {
	// Start a database transaction
	tx, err := pgConn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Create outbox producer with the transaction
	executor, err := producer.NewPgxExecutor(tx)
	if err != nil {
		return err
	}

	outboxProducer, err := producer.NewPostgresAdapter(executor, cfg.OutboxPrefix, log)
	if err != nil {
		return err
	}

	// Insert user data
	_, err = tx.Exec(ctx, "INSERT INTO users (data) VALUES ($1)", userData)
	if err != nil {
		return err
	}

	// Create metadata
	metadata := map[string]string{
		"source":  "user-service",
		"version": "1.0",
	}

	// Emit an outbox event for user creation
	// This will be written to the WAL and picked up by the consumer
	id, _ := uuid.NewV7() // aggregate ID (would typically be the actual user ID)
	eventID, err := outboxProducer.EmitEvent(
		ctx,
		"user",         // aggregate type
		id.String(),    // aggregate ID
		"user.created", // event type
		userData,       // event payload
		metadata,       // event metadata
	)
	if err != nil {
		return err
	}

	log.Info("Emitted user created event", "event_id", eventID)

	// Commit the transaction
	// Both the user insert and the outbox event will be committed atomically
	return tx.Commit(ctx)
}
