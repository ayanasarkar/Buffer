package db

import (
"context"
"fmt"
"time"

"github.com/jackc/pgx/v5"
"github.com/jackc/pgx/v5/pgxpool"
"go-scheduler/internal/models"
)

// TicketStore is the raw-SQL layer against the Prisma-managed Ticket,
// Counter, and Stage tables. go-scheduler only READS and UPDATES rows
// here, it never creates these three tables.
type TicketStore struct {
pool *pgxpool.Pool
}

func NewTicketStore(pool *pgxpool.Pool) *TicketStore {
return &TicketStore{pool: pool}
}

// openProcessingStart returns the started_at of ticketID open
// (ended_at IS NULL) row in ticket_processing_log, or nil if it has none,
// meaning it is not currently PROCESSING or was never marked so.
func (s *TicketStore) openProcessingStart(ctx context.Context, ticketID string) (*time.Time, error) {
row := s.pool.QueryRow(ctx, `
SELECT started_at FROM ticket_processing_log
WHERE ticket_id = $1 AND ended_at IS NULL
ORDER BY started_at DESC LIMIT 1
`, ticketID)
var started time.Time
if err := row.Scan(&started); err != nil {
if err == pgx.ErrNoRows {
return nil, nil
}
return nil, fmt.Errorf("failed to check processing log for ticket %s: %w", ticketID, err)
}
return &started, nil
}

// --- reads ---

func (s *TicketStore) GetTicket(ctx context.Context, ticketID string) (*models.Ticket, error) {
row := s.pool.QueryRow(ctx, `
SELECT "id", "id", "status", "assignedCounterId", "currentStageId", "createdAt"
FROM "Ticket"
WHERE "id" = $1
`, ticketID)

var t models.Ticket
if err := row.Scan(&t.ID, &t.UserID, &t.Status, &t.AssignedCounterID, &t.CurrentStageID, &t.CreatedAt); err != nil {
if err == pgx.ErrNoRows {
return nil, fmt.Errorf("ticket %s not found: %w", ticketID, err)
}
return nil, fmt.Errorf("failed to load ticket %s: %w", ticketID, err)
}

started, err := s.openProcessingStart(ctx, t.ID)
if err != nil {
return nil, err
}
t.ProcessingStartedAt = started
return &t, nil
}

func (s *TicketStore) GetCounter(ctx context.Context, counterID string) (*models.Counter, error) {
row := s.pool.QueryRow(ctx, `
SELECT "id", "stageId", "status"
FROM "Counter"
WHERE "id" = $1
`, counterID)

var c models.Counter
if err := row.Scan(&c.ID, &c.StageID, &c.Status); err != nil {
if err == pgx.ErrNoRows {
return nil, fmt.Errorf("counter %s not found: %w", counterID, err)
}
return nil, fmt.Errorf("failed to load counter %s: %w", counterID, err)
}
return &c, nil
}

// GetStage does not select an estimatedDurationMins column, confirmed
// absent from schema.prisma. EstimatedDurationMins is always
// models.DefaultEstimatedDurationMins until a migration adds the real
// column and this function is updated to select it.
func (s *TicketStore) GetStage(ctx context.Context, stageID string) (*models.Stage, error) {
row := s.pool.QueryRow(ctx, `
SELECT "id", "name"
FROM "Stage"
WHERE "id" = $1
`, stageID)

var st models.Stage
if err := row.Scan(&st.ID, &st.Name); err != nil {
if err == pgx.ErrNoRows {
return nil, fmt.Errorf("stage %s not found: %w", stageID, err)
}
return nil, fmt.Errorf("failed to load stage %s: %w", stageID, err)
}
st.EstimatedDurationMins = models.DefaultEstimatedDurationMins
return &st, nil
}

// ListWaitingTickets returns tickets WAITING at counterID, oldest first.
// ProcessingStartedAt is left nil for these, since a WAITING ticket should
// never have an open processing log row.
func (s *TicketStore) ListWaitingTickets(ctx context.Context, counterID string) ([]models.Ticket, error) {
rows, err := s.pool.Query(ctx, `
SELECT "id", "id", "status", "assignedCounterId", "currentStageId", "createdAt"
FROM "Ticket"
WHERE "assignedCounterId" = $1 AND "status" = $2
ORDER BY "createdAt" ASC
`, counterID, models.TicketWaiting)
if err != nil {
return nil, fmt.Errorf("failed to list waiting tickets for counter %s: %w", counterID, err)
}
defer rows.Close()

var out []models.Ticket
for rows.Next() {
var t models.Ticket
if err := rows.Scan(&t.ID, &t.UserID, &t.Status, &t.AssignedCounterID, &t.CurrentStageID, &t.CreatedAt); err != nil {
return nil, fmt.Errorf("failed to scan waiting ticket: %w", err)
}
out = append(out, t)
}
return out, rows.Err()
}

// GetProcessingTicket returns the ticket currently PROCESSING at
// counterID, or nil if the counter is empty right now, with
// ProcessingStartedAt populated from ticket_processing_log.
func (s *TicketStore) GetProcessingTicket(ctx context.Context, counterID string) (*models.Ticket, error) {
row := s.pool.QueryRow(ctx, `
SELECT "id", "id", "status", "assignedCounterId", "currentStageId", "createdAt"
FROM "Ticket"
WHERE "assignedCounterId" = $1 AND "status" = $2
LIMIT 1
`, counterID, models.TicketProcessing)

var t models.Ticket
if err := row.Scan(&t.ID, &t.UserID, &t.Status, &t.AssignedCounterID, &t.CurrentStageID, &t.CreatedAt); err != nil {
if err == pgx.ErrNoRows {
return nil, nil
}
return nil, fmt.Errorf("failed to load processing ticket for counter %s: %w", counterID, err)
}

started, err := s.openProcessingStart(ctx, t.ID)
if err != nil {
return nil, err
}
t.ProcessingStartedAt = started
return &t, nil
}

func (s *TicketStore) ListOpenCounters(ctx context.Context) ([]models.Counter, error) {
rows, err := s.pool.Query(ctx, `
SELECT "id", "stageId", "status"
FROM "Counter"
WHERE "status" = $1
`, models.CounterOpen)
if err != nil {
return nil, fmt.Errorf("failed to list open counters: %w", err)
}
defer rows.Close()

var out []models.Counter
for rows.Next() {
var c models.Counter
if err := rows.Scan(&c.ID, &c.StageID, &c.Status); err != nil {
return nil, fmt.Errorf("failed to scan open counter: %w", err)
}
out = append(out, c)
}
return out, rows.Err()
}

func (s *TicketStore) ListCountersByStage(ctx context.Context, stageID string) ([]models.Counter, error) {
rows, err := s.pool.Query(ctx, `
SELECT "id", "stageId", "status"
FROM "Counter"
WHERE "stageId" = $1
`, stageID)
if err != nil {
return nil, fmt.Errorf("failed to list counters for stage %s: %w", stageID, err)
}
defer rows.Close()

var out []models.Counter
for rows.Next() {
var c models.Counter
if err := rows.Scan(&c.ID, &c.StageID, &c.Status); err != nil {
return nil, fmt.Errorf("failed to scan counter: %w", err)
}
out = append(out, c)
}
return out, rows.Err()
}

// AvgProcessingMinutes averages actual processing duration from
// ticket_processing_log directly, over rows closed (ended_at set) at
// counterID since the given time.
func (s *TicketStore) AvgProcessingMinutes(ctx context.Context, counterID string, since time.Time) (avgMinutes float64, sampleSize int, err error) {
row := s.pool.QueryRow(ctx, `
SELECT
  COALESCE(AVG(EXTRACT(EPOCH FROM (ended_at - started_at)) / 60.0), 0),
  COUNT(*)
FROM ticket_processing_log
WHERE counter_id = $1
  AND ended_at IS NOT NULL
  AND started_at >= $2
`, counterID, since)

if err := row.Scan(&avgMinutes, &sampleSize); err != nil {
return 0, 0, fmt.Errorf("failed to compute avg processing minutes for counter %s: %w", counterID, err)
}
return avgMinutes, sampleSize, nil
}

// --- writes ---

func (s *TicketStore) SetTicketStatus(ctx context.Context, ticketID string, status models.TicketStatus) error {
_, err := s.pool.Exec(ctx, `UPDATE "Ticket" SET "status" = $1, "updatedAt" = now() WHERE "id" = $2`, status, ticketID)
if err != nil {
return fmt.Errorf("failed to set ticket %s status to %s: %w", ticketID, status, err)
}
return nil
}

// SetTicketProcessing marks a ticket PROCESSING at counterID and opens a
// new ticket_processing_log row to start its stuck-timer clock. Called
// from CALL_NEXT. Wrapped in a transaction so the Ticket update and the
// log insert either both land or neither does.
func (s *TicketStore) SetTicketProcessing(ctx context.Context, ticketID, counterID string, startedAt time.Time) error {
tx, err := s.pool.Begin(ctx)
if err != nil {
return fmt.Errorf("failed to begin tx for ticket %s processing: %w", ticketID, err)
}
defer tx.Rollback(ctx)

if _, err := tx.Exec(ctx, `
UPDATE "Ticket"
SET "status" = $1, "assignedCounterId" = $2, "updatedAt" = now()
WHERE "id" = $3
`, models.TicketProcessing, counterID, ticketID); err != nil {
return fmt.Errorf("failed to mark ticket %s processing at counter %s: %w", ticketID, counterID, err)
}

if _, err := tx.Exec(ctx, `
INSERT INTO ticket_processing_log (ticket_id, counter_id, started_at, ended_at)
VALUES ($1, $2, $3, NULL)
`, ticketID, counterID, startedAt); err != nil {
return fmt.Errorf("failed to open processing log for ticket %s: %w", ticketID, err)
}

return tx.Commit(ctx)
}

// CompleteProcessing closes ticketID open processing_log row, if any,
// setting ended_at = now(). Called from COMPLETE_STUDENT so
// AvgProcessingMinutes has a real duration to average. Safe to call even
// if there is no open row, it just affects zero rows.
func (s *TicketStore) CompleteProcessing(ctx context.Context, ticketID string) error {
_, err := s.pool.Exec(ctx, `
UPDATE ticket_processing_log SET ended_at = now()
WHERE ticket_id = $1 AND ended_at IS NULL
`, ticketID)
if err != nil {
return fmt.Errorf("failed to close processing log for ticket %s: %w", ticketID, err)
}
return nil
}

func (s *TicketStore) SetCounterStatus(ctx context.Context, counterID string, status models.CounterStatus) error {
_, err := s.pool.Exec(ctx, `UPDATE "Counter" SET "status" = $1 WHERE "id" = $2`, status, counterID)
if err != nil {
return fmt.Errorf("failed to set counter %s status to %s: %w", counterID, status, err)
}
return nil
}

// --- live ETA push, reuses go-scheduler own live_eta_updates table ---

func (s *TicketStore) pushLiveETA(ctx context.Context, userID string, eta time.Time, delayMinutes int) error {
_, err := s.pool.Exec(ctx, `
INSERT INTO live_eta_updates (user_id, original_slot, updated_eta, delay_minutes, updated_at)
VALUES ($1, NULL, $2, $3, now())
ON CONFLICT (user_id) DO UPDATE SET
  updated_eta   = EXCLUDED.updated_eta,
  delay_minutes = EXCLUDED.delay_minutes,
  updated_at    = now()
`, userID, eta, delayMinutes)
if err != nil {
return fmt.Errorf("failed to upsert live eta for user %s: %w", userID, err)
}

if _, err := s.pool.Exec(ctx, `SELECT pg_notify('eta_updates', $1)`, userID); err != nil {
return fmt.Errorf("failed to notify eta update for user %s: %w", userID, err)
}
return nil
}

func (s *TicketStore) currentETA(ctx context.Context, userID string) (eta time.Time, delayMinutes int, found bool, err error) {
row := s.pool.QueryRow(ctx, `SELECT updated_eta, delay_minutes FROM live_eta_updates WHERE user_id = $1`, userID)
if err := row.Scan(&eta, &delayMinutes); err != nil {
if err == pgx.ErrNoRows {
return time.Time{}, 0, false, nil
}
return time.Time{}, 0, false, fmt.Errorf("failed to read current eta for user %s: %w", userID, err)
}
return eta, delayMinutes, true, nil
}

func (s *TicketStore) RecalculateCounterETAs(ctx context.Context, counterID string) ([]string, error) {
counter, err := s.GetCounter(ctx, counterID)
if err != nil {
return nil, err
}
stage, err := s.GetStage(ctx, counter.StageID)
if err != nil {
return nil, err
}
tickets, err := s.ListWaitingTickets(ctx, counterID)
if err != nil {
return nil, err
}

now := time.Now()
var pushed []string
for i, t := range tickets {
eta := now.Add(time.Duration(stage.EstimatedDurationMins*(i+1)) * time.Minute)
if err := s.pushLiveETA(ctx, t.UserID, eta, 0); err != nil {
return pushed, err
}
pushed = append(pushed, t.UserID)
}
return pushed, nil
}

func (s *TicketStore) BumpCounterETAs(ctx context.Context, counterID string, addMinutes int) ([]string, error) {
tickets, err := s.ListWaitingTickets(ctx, counterID)
if err != nil {
return nil, err
}

var pushed []string
for _, t := range tickets {
curETA, curDelay, found, err := s.currentETA(ctx, t.UserID)
if err != nil {
return pushed, err
}
var newETA time.Time
if found {
newETA = curETA.Add(time.Duration(addMinutes) * time.Minute)
} else {
newETA = time.Now().Add(time.Duration(addMinutes) * time.Minute)
}
if err := s.pushLiveETA(ctx, t.UserID, newETA, curDelay+addMinutes); err != nil {
return pushed, err
}
pushed = append(pushed, t.UserID)
}
return pushed, nil
}
