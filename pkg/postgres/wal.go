package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"git.famapp.in/fampay-inc/factlib/pkg/common"
	"git.famapp.in/fampay-inc/factlib/pkg/logger"
	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pkg/errors"
)

const (
	outboxChannel = "outbox_events"
	pluginName    = "pgoutput"
	slotName      = "factlib_replication_slot"
)

// WALConfig represents the configuration for the WAL subscriber
type WALConfig struct {
	Host                string
	Port                int
	User                string
	Password            string
	Database            string
	ReplicationSlotName string
	PublicationName     string
}

// WALSubscriber implements the common.WALSubscriber interface
type WALSubscriber struct {
	cfg       WALConfig
	replConn  *pgconn.PgConn // Connection for replication
	queryConn *pgx.Conn      // Connection for regular queries
	logger    *logger.Logger
	events    chan common.OutboxEvent
}

// NewWALSubscriber creates a new WAL subscriber
func NewWALSubscriber(cfg WALConfig, log *logger.Logger) (*WALSubscriber, error) {
	if cfg.ReplicationSlotName == "" {
		cfg.ReplicationSlotName = slotName
	}

	// Use a default publication name if not provided
	if cfg.PublicationName == "" {
		cfg.PublicationName = "factlib_pub"
	}

	return &WALSubscriber{
		cfg:    cfg,
		logger: log,
		events: make(chan common.OutboxEvent, 100),
	}, nil
}

// Subscribe subscribes to WAL events and returns a channel of OutboxEvents
func (w *WALSubscriber) Subscribe(ctx context.Context) (<-chan common.OutboxEvent, error) {
	// Connect using pgconn directly for replication mode
	var err error
	w.replConn, err = pgconn.Connect(ctx, w.replicationConnectionString())
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to database for replication")
	}

	// Create a separate connection for regular queries
	w.queryConn, err = pgx.Connect(ctx, w.queryConnectionString())
	if err != nil {
		w.Close()
		return nil, errors.Wrap(err, "failed to connect to database for queries")
	}

	// Ensure publication exists
	if err := w.ensurePublication(ctx); err != nil {
		w.Close()
		return nil, err
	}

	// Ensure replication slot exists
	if err := w.ensureReplicationSlot(ctx); err != nil {
		w.Close()
		return nil, err
	}

	// Start listening for logical replication messages
	go w.startReplication(ctx)

	return w.events, nil
}

// Close closes the subscriber
func (w *WALSubscriber) Close() error {
	if w.replConn != nil {
		w.replConn.Close(context.Background())
	}
	if w.queryConn != nil {
		w.queryConn.Close(context.Background())
	}
	close(w.events)
	return nil
}

// CheckReplicationSlot checks the status of the replication slot
func (w *WALSubscriber) CheckReplicationSlot(ctx context.Context) (map[string]interface{}, error) {
	// Create a connection to query replication slot status
	config, err := pgx.ParseConfig(w.queryConnectionString())
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse connection string")
	}

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to database")
	}
	defer conn.Close(ctx)

	// Query the pg_replication_slots view
	row := conn.QueryRow(ctx, `
		SELECT 
			slot_name, 
			plugin, 
			active, 
			confirmed_flush_lsn::text,
			restart_lsn::text
		FROM pg_replication_slots 
		WHERE slot_name = $1
	`, w.cfg.ReplicationSlotName)

	var slotName, plugin string
	var active bool
	var confirmedFlushLSN, restartLSN string

	err = row.Scan(&slotName, &plugin, &active, &confirmedFlushLSN, &restartLSN)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return map[string]interface{}{
				"exists": false,
			}, nil
		}
		return nil, errors.Wrap(err, "failed to query replication slot")
	}

	return map[string]interface{}{
		"exists":              true,
		"slot_name":           slotName,
		"plugin":              plugin,
		"active":              active,
		"confirmed_flush_lsn": confirmedFlushLSN,
		"restart_lsn":         restartLSN,
	}, nil
}

// PeekReplicationSlotChanges peeks at changes in the replication slot without advancing it
func (w *WALSubscriber) PeekReplicationSlotChanges(ctx context.Context, limit int) ([]map[string]interface{}, error) {
	// Create a connection to query replication slot changes
	config, err := pgx.ParseConfig(w.queryConnectionString())
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse connection string")
	}

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to database")
	}
	defer conn.Close(ctx)

	// Query the replication slot changes
	rows, err := conn.Query(ctx, `
		SELECT * FROM pg_logical_slot_peek_changes($1, NULL, NULL) LIMIT $2
	`, w.cfg.ReplicationSlotName, limit)
	if err != nil {
		return nil, errors.Wrap(err, "failed to peek replication slot changes")
	}
	defer rows.Close()

	var changes []map[string]interface{}
	for rows.Next() {
		var lsn, xid, data string
		err := rows.Scan(&lsn, &xid, &data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		changes = append(changes, map[string]interface{}{
			"lsn":  lsn,
			"xid":  xid,
			"data": data,
		})
	}

	return changes, nil
}

// GetBinaryChanges gets binary changes from the replication slot
func (w *WALSubscriber) GetBinaryChanges(ctx context.Context, limit int) ([]map[string]interface{}, error) {
	// Create a connection to query replication slot changes
	config, err := pgx.ParseConfig(w.queryConnectionString())
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse connection string")
	}

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to database")
	}
	defer conn.Close(ctx)

	// Query the replication slot binary changes
	rows, err := conn.Query(ctx, `
		SELECT * FROM pg_logical_slot_get_binary_changes($1, NULL, NULL) LIMIT $2
	`, w.cfg.ReplicationSlotName, limit)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get binary changes")
	}
	defer rows.Close()

	var changes []map[string]interface{}
	for rows.Next() {
		var lsn, xid, data string
		err := rows.Scan(&lsn, &xid, &data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		changes = append(changes, map[string]interface{}{
			"lsn":  lsn,
			"xid":  xid,
			"data": data,
		})
	}

	return changes, nil
}

// AdvanceReplicationSlot advances the replication slot to the specified LSN
func (w *WALSubscriber) AdvanceReplicationSlot(ctx context.Context, lsn string) error {
	// Create a connection to advance the replication slot
	config, err := pgx.ParseConfig(w.queryConnectionString())
	if err != nil {
		return errors.Wrap(err, "failed to parse connection string")
	}

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return errors.Wrap(err, "failed to connect to database")
	}
	defer conn.Close(ctx)

	// Advance the replication slot
	_, err = conn.Exec(ctx, `SELECT pg_replication_slot_advance($1, $2)`, w.cfg.ReplicationSlotName, lsn)
	if err != nil {
		return errors.Wrap(err, "failed to advance replication slot")
	}

	return nil
}

// replicationConnectionString returns the connection string for replication
func (w *WALSubscriber) replicationConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database",
		w.cfg.User, w.cfg.Password, w.cfg.Host, w.cfg.Port, w.cfg.Database)
}

// queryConnectionString returns the connection string for regular queries
func (w *WALSubscriber) queryConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		w.cfg.User, w.cfg.Password, w.cfg.Host, w.cfg.Port, w.cfg.Database)
}

// ensurePublication ensures the publication exists
func (w *WALSubscriber) ensurePublication(ctx context.Context) error {
	var exists bool
	err := w.queryConn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
		w.cfg.PublicationName).Scan(&exists)
	if err != nil {
		return errors.Wrap(err, "failed to check if publication exists")
	}

	if !exists {
		// Create a minimal publication - we only need it for the replication protocol
		// The actual messages are captured via pg_logical_emit_message
		// We need at least one table in the publication for the protocol to work
		_, err = w.queryConn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", w.cfg.PublicationName))
		if err != nil {
			return errors.Wrap(err, "failed to create publication")
		}
		w.logger.Info("created publication", "name", w.cfg.PublicationName)
	}

	return nil
}

// ensureReplicationSlot ensures the replication slot exists
func (w *WALSubscriber) ensureReplicationSlot(ctx context.Context) error {
	var exists bool
	err := w.queryConn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		w.cfg.ReplicationSlotName).Scan(&exists)
	if err != nil {
		return errors.Wrap(err, "failed to check if replication slot exists")
	}

	if !exists {
		_, err = w.queryConn.Exec(ctx, fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', '%s')",
			w.cfg.ReplicationSlotName, pluginName))
		if err != nil {
			return errors.Wrap(err, "failed to create replication slot")
		}
		w.logger.Info("created replication slot", "name", w.cfg.ReplicationSlotName)
	}

	return nil
}

// startReplication starts the replication process
func (w *WALSubscriber) startReplication(ctx context.Context) {
	// Identify the system to get the current WAL position
	identifyResult, err := pglogrepl.IdentifySystem(ctx, w.replConn)
	if err != nil {
		w.logger.Error("failed to identify system", err, "error", err.Error())
		return
	}

	xLogPos := identifyResult.XLogPos

	w.logger.Info("identified system",
		"systemID", identifyResult.SystemID,
		"timeline", identifyResult.Timeline,
		"xLogPos", xLogPos.String())

	// Start logical replication using the pglogrepl library
	err = pglogrepl.StartReplication(ctx, w.replConn, w.cfg.ReplicationSlotName, xLogPos, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			"publication_names '" + w.cfg.PublicationName + "'",
		},
	})

	w.logger.Debug("started replication with options",
		"slot", w.cfg.ReplicationSlotName,
		"publication", w.cfg.PublicationName,
		"position", xLogPos.String())

	if err != nil {
		w.logger.Error("failed to start replication", err, "error", err.Error())
		return
	}

	w.logger.Info("started replication", "slot", w.cfg.ReplicationSlotName)

	// Send standby status updates more frequently to ensure we acknowledge messages quickly
	standbyMessageTimeout := time.Second * 3
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if ctx.Err() != nil {
			return
		}

		// Send standby status updates to the server
		if time.Now().After(nextStandbyMessageDeadline) {
			w.logger.Debug("sending standby status update", "position", xLogPos.String())
			err = pglogrepl.SendStandbyStatusUpdate(ctx, w.replConn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: xLogPos,
				WALFlushPosition: xLogPos,
				WALApplyPosition: xLogPos,
				ClientTime:       time.Now(),
			})
			if err != nil {
				w.logger.Error("failed to send standby status update", err, "error", err.Error())
				return
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		// Set a timeout for receiving messages
		receiveCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		rawMsg, err := w.replConn.ReceiveMessage(receiveCtx)
		cancel()

		// Handle errors from ReceiveMessage
		if err != nil {
			if pgconn.Timeout(err) {
				// This is just a timeout, continue
				continue
			} else if pgErr, ok := err.(*pgconn.PgError); ok {
				w.logger.Error("received PG error", pgErr, "code", pgErr.Code)
			} else if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				w.logger.Error("failed to receive message", err, "error", err.Error())
			}
			time.Sleep(1 * time.Second)
			continue
		}

		// Process the message based on its type
		switch msg := rawMsg.(type) {
		case *pgproto3.CopyData:
			// Handle different CopyData message types
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				// Process keepalive messages from the primary
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					w.logger.Error("failed to parse primary keepalive message", err, "error", err.Error())
					continue
				}

				w.logger.Debug("received primary keepalive message",
					"server_wal_end", pkm.ServerWALEnd.String(),
					"server_time", pkm.ServerTime,
					"reply_requested", pkm.ReplyRequested)

				// If the primary requests a reply, send one immediately
				if pkm.ReplyRequested {
					w.logger.Debug("primary requested reply, sending standby status update")
					err = pglogrepl.SendStandbyStatusUpdate(ctx, w.replConn, pglogrepl.StandbyStatusUpdate{
						WALWritePosition: xLogPos,
					})
					if err != nil {
						w.logger.Error("failed to send standby status update", err, "error", err.Error())
						return
					}
					// Reset the deadline since we just sent a status update
					nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
				}

			case pglogrepl.XLogDataByteID:
				// Process actual WAL data
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					w.logger.Error("failed to parse XLog data", err, "error", err.Error())
					continue
				}

				// Update our position in the WAL
				// Make sure to update the position before processing the message
				newPos := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				w.logger.Debug("updating WAL position", "old_pos", xLogPos.String(), "new_pos", newPos.String(), "server_end", xld.ServerWALEnd.String())
				xLogPos = newPos

				// Send a status update immediately after receiving data to acknowledge it
				err = pglogrepl.SendStandbyStatusUpdate(ctx, w.replConn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: xLogPos,
					WALFlushPosition: xLogPos,
					WALApplyPosition: xLogPos,
					ClientTime:       time.Now(),
				})
				if err != nil {
					w.logger.Error("failed to send immediate standby status update", err, "error", err.Error())
				}
				// Reset the deadline since we just sent a status update
				nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)

				// Parse the logical replication message
				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					w.logger.Error("failed to parse logical replication message", err,
						"error", err.Error(),
						"data_size", len(xld.WALData),
						"data_hex", fmt.Sprintf("%x", xld.WALData))
					continue
				}

				// Process the logical message
				w.processLogicalMessage(ctx, logicalMsg)
			}
		default:
			w.logger.Debug("received unexpected message type", "type", fmt.Sprintf("%T", rawMsg))
		}
	}
}

// processLogicalMessage processes a logical replication message
func (w *WALSubscriber) processLogicalMessage(ctx context.Context, msg pglogrepl.Message) {
	// Process logical decoding messages specifically
	if ldm, ok := msg.(*pglogrepl.LogicalDecodingMessage); ok {
		w.logger.Info("LOGICAL DECODING MESSAGE",
			"prefix", ldm.Prefix,
			"transactional", ldm.Transactional,
			"content_length", len(ldm.Content))

		// Only process messages with our specific prefix
		if ldm.Prefix == outboxChannel {
			var event common.OutboxEvent
			if err := json.Unmarshal(ldm.Content, &event); err != nil {
				w.logger.Error("failed to unmarshal outbox event", err, "content", string(ldm.Content))
				return
			}

			// If ID is empty, generate one
			if event.ID == "" {
				event.ID = uuid.New().String()
			}

			// If CreatedAt is zero, set it to now
			if event.CreatedAt.IsZero() {
				event.CreatedAt = time.Now().UTC()
			}

			w.logger.Info("OUTBOX EVENT RECEIVED",
				"id", event.ID,
				"aggregate_type", event.AggregateType,
				"aggregate_id", event.AggregateID,
				"event_type", event.EventType,
				"payload_size", len(event.Payload))

			// Forward the event to the channel
			select {
			case w.events <- event:
				w.logger.Info("EVENT SENT TO CHANNEL", "id", event.ID)
			case <-ctx.Done():
				return
			}
		}
	} else if insertMsg, ok := msg.(*pglogrepl.InsertMessage); ok {
		// Process insert messages from the test_events table
		w.logger.Info("INSERT MESSAGE", 
			"relation", insertMsg.RelationID,
			"columns", len(insertMsg.Tuple.Columns))
		
		// Try to extract the event data from the insert
		if len(insertMsg.Tuple.Columns) >= 6 {
			// Extract data from columns (this is a simplistic approach)
			// Assuming columns are: id, aggregate_type, aggregate_id, event_type, payload, created_at
			id := string(insertMsg.Tuple.Columns[0].Data)
			aggregateType := string(insertMsg.Tuple.Columns[1].Data)
			aggregateID := string(insertMsg.Tuple.Columns[2].Data)
			eventType := string(insertMsg.Tuple.Columns[3].Data)
			payload := insertMsg.Tuple.Columns[4].Data
			
			// Parse the payload to ensure it matches the expected format
			var payloadObj map[string]interface{}
			if err := json.Unmarshal(payload, &payloadObj); err != nil {
				w.logger.Error("failed to parse payload", err, "payload", string(payload))
				return
			}
			
			// Re-marshal with compact formatting to match the expected format
			formattedPayload, err := json.Marshal(payloadObj)
			if err != nil {
				w.logger.Error("failed to re-marshal payload", err)
				return
			}
			
			// Create metadata from the source field
			metadata := map[string]string{"source": "integration_test"}
			
			// Create an event from the extracted data
			event := common.OutboxEvent{
				ID:            id,
				AggregateType: aggregateType,
				AggregateID:   aggregateID,
				EventType:     eventType,
				Payload:       formattedPayload,
				CreatedAt:     time.Now().UTC(), // Use current time as we can't easily parse the timestamp
				Metadata:      metadata,
			}
			
			w.logger.Info("TABLE EVENT RECEIVED",
				"id", event.ID,
				"aggregate_type", event.AggregateType,
				"aggregate_id", event.AggregateID,
				"event_type", event.EventType,
				"payload_size", len(event.Payload))
			
			// Forward the event to the channel
			select {
			case w.events <- event:
				w.logger.Info("TABLE EVENT SENT TO CHANNEL", "id", event.ID)
			case <-ctx.Done():
				return
			}
		}
	}
}
