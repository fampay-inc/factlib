package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/client"
	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/kafka"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"git.famapp.in/fampay-inc/factlib/pkg/postgres"
	"git.famapp.in/fampay-inc/factlib/pkg/worker"
)

func main() {
	// Parse command line flags
	mode := flag.String("mode", "both", "Mode to run in: client, worker, or both")
	flag.Parse()

	// Initialize logger
	log := logger.New(logger.Config{
		Level:      "debug",
		JSONOutput: false,
		WithCaller: true,
	})

	// Create context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-signalCh
		log.Info("received signal, shutting down", "signal", sig.String())
		cancel()
	}()

	// Run based on mode
	switch *mode {
	case "client":
		runClient(ctx, log)
	case "worker":
		runWorker(ctx, log)
	case "both":
		runClient(ctx, log)
		runWorker(ctx, log)
	default:
		log.Fatal("invalid mode", nil, "mode", *mode)
	}

	// Wait for context cancellation
	<-ctx.Done()
	log.Info("shutting down")
}

func runClient(ctx context.Context, log *logger.Logger) {
	log.Info("initializing client")

	// Initialize Postgres client
	postgresClient, err := client.NewPostgresClient(ctx, client.PostgresConfig{
		ConnectionString: "postgres://postgres:postgres@localhost:5432/outbox_example?sslmode=disable",
	}, log)
	if err != nil {
		log.Fatal("failed to create Postgres client", err)
	}
	defer postgresClient.Close()

	// Start a goroutine to publish example events periodically
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Create a sample payload
				payload := []byte(`{"message":"Hello, World!","timestamp":"` + time.Now().String() + `"}`)
				
				// Define metadata
				metadata := map[string]string{
					"source":   "example",
					"priority": "high",
				}

				// Publish the event
				id, err := postgresClient.Publish(
					ctx,
					"user",                    // Aggregate type
					"user123",                 // Aggregate ID
					"user.created",            // Event type
					payload,                   // Payload
					metadata,                  // Metadata
				)
				if err != nil {
					log.Error("failed to publish event", err)
					continue
				}
				
				log.Info("published event", "id", id)
			}
		}
	}()
}

func runWorker(ctx context.Context, log *logger.Logger) {
	log.Info("initializing worker")

	// Initialize WAL subscriber
	walSubscriber, err := postgres.NewWALSubscriber(postgres.WALConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "postgres",
		Database: "outbox_example",
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

	// Register handlers
	err = outboxWorker.Register(
		"user",                       // Aggregate type
		"user-events",                // Kafka topic
		worker.NewExampleHandler(log), // Handler
	)
	if err != nil {
		log.Fatal("failed to register handler", err)
	}

	// Start the worker
	if err := outboxWorker.Start(ctx); err != nil {
		log.Fatal("failed to start worker", err)
	}
	defer outboxWorker.Close()

	log.Info("worker started")
}
