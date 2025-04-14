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
	publicationName = "factlib_publication"
	slotName      = "factlib_replication_slot"
)

// WALConfig represents the configuration for the WAL subscriber
type WALConfig struct {
	Host         string
	Port         int
	User         string
	Password     string
	Database     string
	ReplicationSlotName string
	PublicationName     string
}

// WALSubscriber implements the common.WALSubscriber interface
type WALSubscriber struct {
	cfg       WALConfig
	replConn  *pgconn.PgConn     // Connection for replication
	queryConn *pgx.Conn         // Connection for regular queries
	logger    *logger.Logger
	events    chan common.OutboxEvent
}

// NewWALSubscriber creates a new WAL subscriber
func NewWALSubscriber(cfg WALConfig, log *logger.Logger) (*WALSubscriber, error) {
	if cfg.ReplicationSlotName == "" {
		cfg.ReplicationSlotName = slotName
	}
	
	if cfg.PublicationName == "" {
		cfg.PublicationName = publicationName
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
	
	// Start logical replication
	err = pglogrepl.StartReplication(ctx, w.replConn, w.cfg.ReplicationSlotName, xLogPos, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			fmt.Sprintf("proto_version=1"),
			fmt.Sprintf("publication_names=%s", w.cfg.PublicationName),
		},
	})
	if err != nil {
		w.logger.Error("failed to start replication", err, "error", err.Error())
		return
	}
	
	w.logger.Info("started replication", "slot", w.cfg.ReplicationSlotName)
	
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
	
	for {
		if ctx.Err() != nil {
			return
		}
		
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, w.replConn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: xLogPos,
			})
			if err != nil {
				w.logger.Error("failed to send standby status update", err, "error", err.Error())
				return
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}
		
		receiveCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		rawMsg, err := w.replConn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if pgErr, ok := err.(*pgconn.PgError); ok {
				w.logger.Error("received PG error", pgErr, "code", pgErr.Code)
			} else if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				w.logger.Error("failed to receive message", err, "error", err.Error())
			}
			time.Sleep(1 * time.Second)
			continue
		}
		
		if copyData, ok := rawMsg.(*pgproto3.CopyData); ok {
			switch copyData.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
				if err != nil {
					w.logger.Error("failed to parse primary keepalive message", err, "error", err.Error())
					continue
				}
				if pkm.ReplyRequested {
					err = pglogrepl.SendStandbyStatusUpdate(ctx, w.replConn, pglogrepl.StandbyStatusUpdate{
						WALWritePosition: xLogPos,
					})
					if err != nil {
						w.logger.Error("failed to send standby status update", err, "error", err.Error())
						return
					}
					nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
				}
				
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
				if err != nil {
					w.logger.Error("failed to parse XLog data", err, "error", err.Error())
					continue
				}
				
				xLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				
				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					w.logger.Error("failed to parse logical replication message", err, "error", err.Error())
					continue
				}
				
				w.processLogicalMessage(ctx, logicalMsg)
			}
		}
	}
}

// processLogicalMessage processes a logical replication message
func (w *WALSubscriber) processLogicalMessage(ctx context.Context, msg pglogrepl.Message) {
	switch msg := msg.(type) {
	case *pglogrepl.LogicalDecodingMessage:
		if msg.Prefix == outboxChannel {
			var event common.OutboxEvent
			if err := json.Unmarshal(msg.Content, &event); err != nil {
				w.logger.Error("failed to unmarshal outbox event", err, "content", string(msg.Content))
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
			
			w.logger.Debug("received outbox event", 
				"id", event.ID, 
				"aggregate_type", event.AggregateType,
				"event_type", event.EventType)
			
			select {
			case w.events <- event:
			case <-ctx.Done():
				return
			}
		}
	}
}
