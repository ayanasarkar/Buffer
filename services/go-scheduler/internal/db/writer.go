package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go-scheduler/internal/models"
)

type Writer struct {
	pool *pgxpool.Pool
}

func NewWriter(ctx context.Context, url string) (*Writer, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}
	return &Writer{pool: pool}, nil
}

// Pool exposes the underlying connection pool so other packages (the new
// internal/db/tickets.go, specifically) can run their own queries without
// opening a second pool. NEW -- everything else in this file is
// unchanged from the original.
func (w *Writer) Pool() *pgxpool.Pool {
	return w.pool
}

func (w *Writer) Init(ctx context.Context) error {
	_, err := w.pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS scheduled_users (
id                   SERIAL PRIMARY KEY,
user_id              TEXT NOT NULL,
name                 TEXT NOT NULL,
user_group           TEXT NOT NULL,
fairness_score       DOUBLE PRECISION,
slot_id              TEXT,
slot_start           TIMESTAMPTZ,
slot_end             TIMESTAMPTZ,
unscheduled          BOOLEAN NOT NULL DEFAULT FALSE,
unscheduled_reason   TEXT,
swapped_with_user_id TEXT,
swap_reason          TEXT,
created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
)
`)
	if err != nil {
		return fmt.Errorf("failed to create scheduled_users table: %w", err)
	}
	return nil
}

// WriteScheduledUser inserts one final scheduling result. It derives its
// own short timeout at the moment the write actually starts, rather than
// trusting a deadline set by the caller -- a write attempted several
// minutes after the writer was constructed must not inherit a stopwatch
// that started back at program launch. That was the bug in the previous
// version of this file.
func (w *Writer) WriteScheduledUser(ctx context.Context, su models.ScheduledUser) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var slotID, slotStart, slotEnd any
	if su.AssignedSlot != nil {
		slotID = su.AssignedSlot.ID
		slotStart = su.AssignedSlot.StartTime
		slotEnd = su.AssignedSlot.EndTime
	}

	_, err := w.pool.Exec(ctx, `
INSERT INTO scheduled_users (
user_id, name, user_group, fairness_score,
slot_id, slot_start, slot_end,
unscheduled, unscheduled_reason,
swapped_with_user_id, swap_reason
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
`,
		su.UserID, su.Name, string(su.Group), su.FairnessScore,
		slotID, slotStart, slotEnd,
		su.Unscheduled, su.UnscheduledReason,
		su.SwappedWithUserID, su.SwapReason,
	)
	if err != nil {
		return fmt.Errorf("failed to insert scheduled user %s: %w", su.UserID, err)
	}
	return nil
}

// Run drains `in` until it's closed (see the earlier fix's comment for why
// it must not select on ctx.Done()). Pass a context with NO deadline of its
// own here -- WriteScheduledUser now applies its own per-call timeout, so
// this ctx should just be a base (context.Background()), not a clock.
func (w *Writer) Run(ctx context.Context, in <-chan models.ScheduledUser) <-chan error {
	errs := make(chan error, 1)

	go func() {
		defer close(errs)
		for su := range in {
			if err := w.WriteScheduledUser(ctx, su); err != nil {
				select {
				case errs <- err:
				default:
				}
			}
		}
	}()

	return errs
}

func (w *Writer) Close() {
	w.pool.Close()
}

// InitETATable creates the live_eta_updates table if it doesn't exist.
func (w *Writer) InitETATable(ctx context.Context) error {
	_, err := w.pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS live_eta_updates (
user_id       TEXT PRIMARY KEY,
original_slot TEXT,
updated_eta   TIMESTAMPTZ NOT NULL,
delay_minutes INTEGER NOT NULL,
updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
)
`)
	if err != nil {
		return fmt.Errorf("failed to create live_eta_updates table: %w", err)
	}
	return nil
}

// WriteETAUpdate upserts a live ETA and sends a Postgres NOTIFY so Express
// (which owns the WebSocket to the frontend) can LISTEN and push it on,
// without Go ever talking to the frontend directly.
func (w *Writer) WriteETAUpdate(ctx context.Context, u models.ETAUpdate) error {
	var slotID any
	if u.OriginalSlot != nil {
		slotID = u.OriginalSlot.ID
	}

	_, err := w.pool.Exec(ctx, `
INSERT INTO live_eta_updates (user_id, original_slot, updated_eta, delay_minutes, updated_at)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (user_id) DO UPDATE SET
updated_eta   = EXCLUDED.updated_eta,
delay_minutes = EXCLUDED.delay_minutes,
updated_at    = now()
`, u.UserID, slotID, u.UpdatedETA, u.DelayMinutes)
	if err != nil {
		return fmt.Errorf("failed to upsert eta update for %s: %w", u.UserID, err)
	}

	_, err = w.pool.Exec(ctx, `SELECT pg_notify('eta_updates', $1)`, u.UserID)
	if err != nil {
		return fmt.Errorf("failed to notify eta update for %s: %w", u.UserID, err)
	}
	return nil
}

// InitProcessingLog creates the ticket_processing_log table if it does not
// already exist. This is a go-scheduler-owned side table, like
// live_eta_updates, NOT part of Prisma schema -- it exists because the
// real "Ticket" table has no processingStartedAt column, confirmed absent
// from schema.prisma. Tracking start/end here means the Stuck alert and
// the peer-comparison delay check work today, without waiting on a
// migration or touching any Prisma-managed table.
func (w *Writer) InitProcessingLog(ctx context.Context) error {
_, err := w.pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS ticket_processing_log (
id         SERIAL PRIMARY KEY,
ticket_id  TEXT NOT NULL,
counter_id TEXT NOT NULL,
started_at TIMESTAMPTZ NOT NULL,
ended_at   TIMESTAMPTZ
)
`)
if err != nil {
return fmt.Errorf("failed to create ticket_processing_log table: %w", err)
}
_, err = w.pool.Exec(ctx, `
CREATE INDEX IF NOT EXISTS idx_processing_log_open
ON ticket_processing_log (ticket_id) WHERE ended_at IS NULL
`)
if err != nil {
return fmt.Errorf("failed to create ticket_processing_log index: %w", err)
}
return nil
}
