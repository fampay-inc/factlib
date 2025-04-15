package postgres

import (
	"context"
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
	"google.golang.org/protobuf/proto"
)

const (
	pluginName = "pgoutput"
)

// WALConfig represents the configuration for the WAL subscriber
type WALConfig struct {
	DatabaseURL         string
	ReplicationSlotName string
	PublicationName     string
	OutboxPrefix        string // Prefix for logical decoding messages
}

// WALSubscriber implements the common.WALSubscriber interface
type WALSubscriber struct {
	cfg       WALConfig
	replConn  *pgconn.PgConn // Connection for replication
	queryConn *pgx.Conn      // Connection for regular queries
	logger    *logger.Logger
	events    chan *common.OutboxEvent
}

// NewWALSubscriber creates a new WAL subscriber
func NewWALSubscriber(cfg WALConfig, log *logger.Logger) (*WALSubscriber, error) {
	return &WALSubscriber{
		cfg:    cfg,
		logger: log,
		events: make(chan *common.OutboxEvent, 100),
	}, nil
}

// Subscribe subscribes to WAL events and returns a channel of OutboxEvents
func (w *WALSubscriber) Subscribe(ctx context.Context) (<-chan *common.OutboxEvent, error) {
	// Connect using pgconn directly for replication mode
	var err error
	w.replConn, err = pgconn.Connect(ctx, w.replicationConnectionString())
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to database for replication")
	}

	// Create a separate connection for regular queries
	w.queryConn, err = pgx.Connect(ctx, w.cfg.DatabaseURL)
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
	config, err := pgx.ParseConfig(w.cfg.DatabaseURL)

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
	w.logger.Debug("replication slot status", "exists", true, "slot_name", slotName, "plugin", plugin, "active", active, "confirmed_flush_lsn", confirmedFlushLSN, "restart_lsn", restartLSN)

	return map[string]interface{}{
		"exists":              true,
		"slot_name":           slotName,
		"plugin":              plugin,
		"active":              active,
		"confirmed_flush_lsn": confirmedFlushLSN,
		"restart_lsn":         restartLSN,
	}, nil
}

// replicationConnectionString returns the connection string for replication
func (w *WALSubscriber) replicationConnectionString() string {
	return fmt.Sprintf("%s?replication=database", w.cfg.DatabaseURL)
}

// ensurePublication ensures the publication exists
func (w *WALSubscriber) ensurePublication(ctx context.Context) error {
	var exists bool
	err := w.queryConn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)", w.cfg.PublicationName).Scan(&exists)
	if err != nil {
		return errors.Wrap(err, "failed to check if publication exists")
	}

	if !exists {
		_, err = w.queryConn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s", w.cfg.PublicationName))
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
	err := w.queryConn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", w.cfg.ReplicationSlotName).Scan(&exists)
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
			fmt.Sprintf("publication_names '%s'", w.cfg.PublicationName),
			"messages 'true'",
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
	standbyMessageTimeout := time.Second * 5
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
		receiveCtx, cancel := context.WithTimeout(ctx, standbyMessageTimeout)
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

				// w.logger.Debug("received primary keepalive message",
				// 	"server_wal_end", pkm.ServerWALEnd.String(),
				// 	"server_time", pkm.ServerTime,
				// 	"reply_requested", pkm.ReplyRequested)

				if pkm.ReplyRequested {
					// w.logger.Debug("primary requested reply, sending standby status update")
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
				// w.logger.Debug("updating WAL position", "old_pos", xLogPos.String(), "new_pos", newPos.String(), "server_end", xld.ServerWALEnd.String())
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
			// w.logger.Debug("received unexpected message type", "type", fmt.Sprintf("%T", rawMsg))
		}
	}
}

// handleProtoMessage processes a Protobuf message from the notification
func (w *WALSubscriber) handleProtoMessage(ctx context.Context, content []byte) {
	// Create a pointer to a protobuf OutboxEvent
	event := &common.OutboxEvent{}
	if err := proto.Unmarshal(content, event); err != nil {
		w.logger.Error("Failed to unmarshal Protobuf event", err, "content_length", len(content))
		return
	}
	if event.Id == "" {
		event.Id = uuid.New().String()
	}
	if event.CreatedAt == 0 {
		event.CreatedAt = time.Now().Unix()
	}
	w.logger.Info("Protobuf event received",
		"id", event.Id,
		"aggregate_type", event.AggregateType,
		"aggregate_id", event.AggregateId,
		"event_type", event.EventType,
		"payload_size", len(event.Payload))
	select {
	case w.events <- event:
		w.logger.Info("Protobuf event sent to channel", "id", event.Id)
	case <-ctx.Done():
		return
	}
}

func (w *WALSubscriber) processLogicalMessage(ctx context.Context, msg pglogrepl.Message) {
	// Process logical decoding messages specifically
	if ldm, ok := msg.(*pglogrepl.LogicalDecodingMessage); ok {
		w.logger.Info("Logical decoding message received",
			"prefix", ldm.Prefix,
			"transactional", ldm.Transactional,
			"content_length", len(ldm.Content))

		w.logger.Debug("Checking message prefix", "message_prefix", ldm.Prefix, "configured_prefix", w.cfg.OutboxPrefix)

		if ldm.Prefix == w.cfg.OutboxPrefix {
			w.handleProtoMessage(ctx, ldm.Content)
		}
	} else {
		w.logger.Debug("Received unexpected message type", "type", fmt.Sprintf("%T", msg))
	}
}
