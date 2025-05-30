package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	pb "git.famapp.in/fampay-inc/factlib/pkg/proto"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"google.golang.org/protobuf/proto"
)

func main() {
	// PostgreSQL connection string
	connString := "postgres://postgres:postgres@localhost:5432/westeros?replication=database"
	if connString == "" {
		log.Fatal("DATABASE_URL environment variable must be set")
	}

	// Create a replication connection
	conn, err := pgconn.Connect(context.Background(), connString)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer conn.Close(context.Background())

	// Identify the system
	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		log.Fatalf("IdentifySystem failed: %v", err)
	}
	log.Printf("SystemID: %s Timeline: %d XLogPos: %d DBName: %s", sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)

	// Create a slot (if it doesn't exist)
	slotName := "factlib_slot"
	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		log.Printf("CreateReplicationSlot failed (may already exist): %v", err)
	}

	// Get last lsn
	masterDbConn, err := pgx.Connect(context.Background(), "postgres://postgres:postgres@localhost:5432/westeros")
	if err != nil {
		log.Fatalf("Failed to connect for querying: %v", err)
	}
	defer masterDbConn.Close(context.Background())
	var lastLSN pglogrepl.LSN
	err = masterDbConn.QueryRow(context.Background(), "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1;", slotName).Scan(&lastLSN)
	if err != nil {
		log.Fatalf("Failed to get last LSN: %v", err)
	}

	// Start replication
	err = pglogrepl.StartReplication(
		context.Background(),
		conn,
		slotName,
		lastLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				"publication_names 'facts'",
				"messages 'true'",
			},
		})
	if err != nil {
		log.Fatalf("StartReplication failed: %v", err)
	}

	ctx := context.Background()
	// Get a regular connection to query the current LSN
	regularConn, err := pgx.Connect(ctx, connString)
	if err != nil {
		log.Fatalf("Failed to connect for querying: %v", err)
	}
	defer regularConn.Close(ctx)

	// Main replication loop
	clientXLogPos := sysident.XLogPos
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
			if err != nil {
				log.Fatalf("SendStandbyStatusUpdate failed: %v", err)
			}
			log.Printf("Sent Standby status message at %s", clientXLogPos.String())
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctxMsg, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err := conn.ReceiveMessage(ctxMsg)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			log.Fatalf("ReceiveMessage failed: %v", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Fatalf("ParsePrimaryKeepaliveMessage failed: %v", err)
				}
				log.Printf("Primary Keepalive Message: %+v", pkm)

				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Fatalf("ParseXLogData failed: %v", err)
				}

				log.Printf("XLogData: WALStart %s ServerWALEnd %s ServerTime %s", xld.WALStart, xld.ServerWALEnd, xld.ServerTime)

				// Process the logical replication message
				message, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					log.Fatalf("Parse logical replication message: %v", err)
				}

				switch message := message.(type) {
				case *pglogrepl.RelationMessage:
					// Relation message - describes a table structure
					log.Printf("Relation message: %+v", message)

				case *pglogrepl.InsertMessage:
					// Insert message - contains actual inserted data
					log.Printf("Insert message: %+v", message)

				case *pglogrepl.UpdateMessage:
					// Update message - contains updated data
					log.Printf("Update message: %+v", message)

				case *pglogrepl.DeleteMessage:
					// Delete message - contains deleted data
					log.Printf("Delete message: %+v", message)

				case *pglogrepl.TruncateMessage:
					// Truncate message
					log.Printf("Truncate message: %+v", message)

				case *pglogrepl.LogicalDecodingMessage:
					// This is what we're looking for - messages emitted via pg_logical_emit_message
					p := pb.OutboxEvent{}
					err = proto.Unmarshal(message.Content, &p)
					if err != nil {
						log.Fatalf("Failed to unmarshal OutboxEvent: %v", err)
					}
					fmt.Printf("  Prefix: %s\n", message.Prefix)
					fmt.Printf("  Content: %s\n", string(p.Payload))
					fmt.Printf("  Transaction ID: %s\n", message.LSN.String())
					fmt.Printf("  Raw message: %s\n", hex.EncodeToString(message.Content))

				case *pglogrepl.BeginMessage:
					// Begin message - marks the start of a transaction
					log.Printf("Begin message: %+v", message)

				case *pglogrepl.CommitMessage:
					// Commit message - marks the end of a transaction
					log.Printf("Commit message: %+v", message)

				default:
					log.Printf("Unknown message type: %T", message)
				}

				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		default:
			log.Printf("Received unexpected message: %T", msg)
		}
	}
}
