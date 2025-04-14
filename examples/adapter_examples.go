package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"git.famapp.in/fampay-inc/factlib/pkg/client"
	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver for database/sql
)

type UserCreatedEvent struct {
	UserID    string `json:"user_id"`
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

// Example showing how to use factlib with an existing pgx.Conn
func ExampleWithPgxConn() {
	// Initialize logger
	log := logger.New(logger.Config{
		Level:      "debug",
		JSONOutput: false,
		WithCaller: true,
	})

	// This would be your existing pgx connection
	conn, err := pgx.Connect(context.Background(), "postgres://postgres:postgres@localhost:5432/yourdb")
	if err != nil {
		log.Fatal("failed to connect to database", err)
	}
	defer conn.Close(context.Background())

	// Create an adapter for your existing connection
	connAdapter := client.NewPgxConnAdapter(conn, log)

	// Create the payload for your event
	userCreated := UserCreatedEvent{
		UserID:    "user123",
		Email:     "user@example.com",
		FirstName: "John",
		LastName:  "Doe",
	}
	payload, err := json.Marshal(userCreated)
	if err != nil {
		log.Fatal("failed to marshal event", err)
	}

	// Emit the event using the adapter
	eventID, err := client.EmitEvent(
		context.Background(),
		connAdapter,
		"user",           // aggregate type
		"user123",        // aggregate ID
		"user.created",   // event type
		payload,          // payload
		map[string]string{"source": "api"}, // metadata
	)
	if err != nil {
		log.Fatal("failed to emit event", err)
	}

	fmt.Printf("Emitted event with ID: %s\n", eventID)
}

// Example showing how to use factlib within an existing transaction
func ExampleWithinTransaction() {
	// Initialize logger
	log := logger.New(logger.Config{
		Level:      "debug",
		JSONOutput: false,
		WithCaller: true,
	})

	// This would be your existing pgx pool
	pool, err := pgxpool.New(context.Background(), "postgres://postgres:postgres@localhost:5432/yourdb")
	if err != nil {
		log.Fatal("failed to create connection pool", err)
	}
	defer pool.Close()

	// Begin a transaction (this would be part of your existing code)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		log.Fatal("failed to begin transaction", err)
	}
	
	// Create transaction adapter
	txAdapter := client.NewPgxTxAdapter(tx, log)

	// In your existing code, you'd perform your business logic...
	_, err = tx.Exec(context.Background(), "INSERT INTO users (id, email, first_name, last_name) VALUES ($1, $2, $3, $4)",
		"user123", "user@example.com", "John", "Doe")
	if err != nil {
		tx.Rollback(context.Background())
		log.Fatal("failed to insert user", err)
	}

	// Now emit the outbox event as part of the same transaction
	event := common.OutboxEvent{
		AggregateType: "user",
		AggregateID:   "user123",
		EventType:     "user.created",
		Payload: []byte(`{
			"user_id": "user123",
			"email": "user@example.com",
			"first_name": "John",
			"last_name": "Doe"
		}`),
		Metadata: map[string]string{"source": "api"},
	}

	if err := txAdapter.ExecOutboxEvent(context.Background(), event); err != nil {
		tx.Rollback(context.Background())
		log.Fatal("failed to emit outbox event", err)
	}

	// Commit the transaction
	if err := tx.Commit(context.Background()); err != nil {
		log.Fatal("failed to commit transaction", err)
	}
}

// Example showing how to use factlib with database/sql
func ExampleWithDatabaseSql() {
	// Initialize logger
	log := logger.New(logger.Config{
		Level:      "debug",
		JSONOutput: false,
		WithCaller: true,
	})

	// This would be your existing database/sql.DB
	db, err := sql.Open("pgx", "postgres://postgres:postgres@localhost:5432/yourdb")
	if err != nil {
		log.Fatal("failed to connect to database", err)
	}
	defer db.Close()

	// Create an adapter for database operations directly if needed
	// We'll use the transaction adapter for this example

	// Start a transaction
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		log.Fatal("failed to begin transaction", err)
	}

	// Create transaction adapter
	txAdapter := client.NewSqlTxAdapter(tx, log)

	// In your existing code, you'd perform your business logic...
	_, err = tx.ExecContext(context.Background(), 
		"INSERT INTO orders (id, customer_id, amount) VALUES ($1, $2, $3)",
		"order456", "customer789", 129.99)
	if err != nil {
		tx.Rollback()
		log.Fatal("failed to insert order", err)
	}

	// Emit the outbox event as part of the same transaction
	orderCreated := map[string]interface{}{
		"order_id":    "order456",
		"customer_id": "customer789",
		"amount":      129.99,
		"items": []map[string]interface{}{
			{
				"product_id": "prod123",
				"quantity":   2,
				"price":      64.99,
			},
		},
	}

	payload, err := json.Marshal(orderCreated)
	if err != nil {
		tx.Rollback()
		log.Fatal("failed to marshal event", err)
	}

	// Emit the event using the transaction adapter
	eventID, err := client.EmitEvent(
		context.Background(),
		txAdapter,
		"order",          // aggregate type
		"order456",       // aggregate ID
		"order.created",  // event type
		payload,          // payload
		map[string]string{"region": "us-west"}, // metadata
	)
	if err != nil {
		tx.Rollback()
		log.Fatal("failed to emit event", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Fatal("failed to commit transaction", err)
	}

	fmt.Printf("Emitted order.created event with ID: %s\n", eventID)
}
