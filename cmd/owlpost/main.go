package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"git.famapp.in/fampay-inc/factlib/pkg/metrics"

	healthHttp "git.famapp.in/fampay-inc/factlib/cmd/owlpost/delivery/health"
	owlpostmetrics "git.famapp.in/fampay-inc/factlib/cmd/owlpost/delivery/metrics"
	"git.famapp.in/fampay-inc/factlib/cmd/owlpost/utils"
	"git.famapp.in/fampay-inc/factlib/pkg/outbox/consumer"
	"github.com/jackc/pgx/v5"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := utils.GetAppLogger()

	// Register metrics as early as possible
	cfg := utils.GetConfig()
	metrics.Register()

	// Start Prometheus metrics server using delivery/metrics
	owlpostmetrics.StartMetricsServer(cfg.MetricsPort)

	// Set up signal handling
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		var once sync.Once
		for sig := range sigs {
			logger.Infof("Received signal: %v", sig)
			once.Do(func() {
				logger.Info("Initiating shutdown...")
				cancel()
			})
		}
	}()

	logger.Infof("Starting owlpost application for: %s", utils.GetConfig().SrcSvc)

	pgConn, err := pgx.Connect(ctx, cfg.MasterDbURL)
	if err != nil {
		logger.Errorf("Failed to connect to database: %v", err)
		return
	}
	defer func() {
		if err := pgConn.Close(ctx); err != nil {
			logger.Errorf("Failed to close database connection: %v", err)
		} else {
			logger.Infof("Database connection closed successfully.")
		}
	}()
	logger.Info("Connected to database successfully.")
	kafkaConfig := consumer.KafkaConfig{
		BootstrapServers: cfg.KafkaBrokers,
		ClientID:         cfg.Name + "." + cfg.SrcSvc,
		SSL:              cfg.KafkaSSL,
		User:             &cfg.KafkaUsername,
		Password:         &cfg.KafkaPassword,
	}
	kafkaAdapter, err := consumer.NewKafkaAdapter(kafkaConfig, logger)
	if err != nil {
		logger.Errorf("Failed to create Kafka adapter: %v", err)
		return
	}
	defer kafkaAdapter.Close()

	// Set up outbox consumer with processing tracking
	consumerConfig := consumer.Config{
		ConnectionString:    cfg.MasterDbURL,
		WalPrefix:           cfg.WalPrefix,
		ReplicationSlotName: cfg.ReplicationSlotName,
		PublicationName:     cfg.PublicationName,
	}

	outboxConsumer, err := consumer.NewOutboxConsumer(ctx, consumerConfig, logger)
	if err != nil {
		logger.Errorf("Failed to create outbox consumer: %v", err)
		return
	}
	defer outboxConsumer.Stop()

	// Register handler for user events
	outboxConsumer.RegisterHandler(cfg.WalPrefix, consumer.KafkaEventHandler(kafkaAdapter, logger))
	outboxConsumer.RegiserHandlerAck(kafkaAdapter.Acks)

	if err := outboxConsumer.Start(ctx); err != nil {
		logger.Errorf("Failed to start outbox consumer: %v", err)
		return
	}

	// Start health server
	go healthHttp.WalHealthServer(ctx, outboxConsumer, cfg)
	<-ctx.Done()
	logger.Info("owlpost application exiting gracefully.")
}
