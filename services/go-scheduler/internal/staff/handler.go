package staff

import (
"context"
"log"
"time"

"go-scheduler/internal/db"
"go-scheduler/internal/models"
"go-scheduler/internal/queue"
)

type Handler struct {
store    *db.TicketStore
notifier *queue.Notifier
}

func NewHandler(store *db.TicketStore, notifier *queue.Notifier) *Handler {
return &Handler{store: store, notifier: notifier}
}

// OnComplete: COMPLETE_STUDENT, remove from processing, close the
// processing-time log so AvgProcessingMinutes has a real duration for
// this ticket, then recalc ETAs.
func (h *Handler) OnComplete(ctx context.Context, evt models.StudentActionEvent) {
if evt.TicketID == "" {
evt.TicketID = evt.LegacyUserID // CONFIRMED: Express sends this as "user_id", see models/staff.go
}
if err := h.store.SetTicketStatus(ctx, evt.TicketID, models.TicketCompleted); err != nil {
log.Printf("staff: COMPLETE_STUDENT ticket=%s failed: %v", evt.TicketID, err)
return
}
if err := h.store.CompleteProcessing(ctx, evt.TicketID); err != nil {
log.Printf("staff: COMPLETE_STUDENT ticket=%s: closing processing log failed, averages may be off: %v", evt.TicketID, err)
}
log.Printf("staff: COMPLETE_STUDENT ticket=%s counter=%s", evt.TicketID, evt.CounterID)

pushed, err := h.store.RecalculateCounterETAs(ctx, evt.CounterID)
if err != nil {
log.Printf("staff: COMPLETE_STUDENT ticket=%s: eta recalc failed: %v", evt.TicketID, err)
return
}
log.Printf("staff: COMPLETE_STUDENT ticket=%s: recalculated ETAs for %d waiting users at counter=%s", evt.TicketID, len(pushed), evt.CounterID)
}

// OnSkip: SKIP_STUDENT, drop from the active queue, recalc ETAs for
// everyone behind them. Also defensively closes any open processing log
// row for this ticket, harmless if none exists.
func (h *Handler) OnSkip(ctx context.Context, evt models.StudentActionEvent) {
if evt.CounterID == "" {
if ticket, err := h.store.GetTicket(ctx, evt.TicketID); err != nil {
log.Printf("staff: SKIP_STUDENT ticket=%s: no counterId on the wire, and ticket lookup failed too: %v", evt.TicketID, err)
} else if ticket.AssignedCounterID != nil {
evt.CounterID = *ticket.AssignedCounterID
}
}
if err := h.store.SetTicketStatus(ctx, evt.TicketID, models.TicketSkipped); err != nil {
log.Printf("staff: SKIP_STUDENT ticket=%s failed: %v", evt.TicketID, err)
return
}
if err := h.store.CompleteProcessing(ctx, evt.TicketID); err != nil {
log.Printf("staff: SKIP_STUDENT ticket=%s: closing processing log failed: %v", evt.TicketID, err)
}

pushed, err := h.store.RecalculateCounterETAs(ctx, evt.CounterID)
if err != nil {
log.Printf("staff: SKIP_STUDENT ticket=%s: eta recalc failed: %v", evt.TicketID, err)
return
}
log.Printf("staff: SKIP_STUDENT ticket=%s: recalculated ETAs for %d waiting users at counter=%s", evt.TicketID, len(pushed), evt.CounterID)
}

// OnRestore: RESTORE_STUDENT, reinsert a previously skipped student,
// recalc ETAs.
//
// OPEN QUESTION: sets status back to WAITING without touching createdAt,
// so ListWaitingTickets createdAt-ascending order puts them back near
// their original place in line, not at the back. Confirm with the team
// whether a restored student should keep their place or go to the back.
func (h *Handler) OnRestore(ctx context.Context, evt models.StudentActionEvent) {
if evt.CounterID == "" {
if ticket, err := h.store.GetTicket(ctx, evt.TicketID); err != nil {
log.Printf("staff: RESTORE_STUDENT ticket=%s: no counterId on the wire, and ticket lookup failed too: %v", evt.TicketID, err)
} else if ticket.AssignedCounterID != nil {
evt.CounterID = *ticket.AssignedCounterID
}
}
if err := h.store.SetTicketStatus(ctx, evt.TicketID, models.TicketWaiting); err != nil {
log.Printf("staff: RESTORE_STUDENT ticket=%s failed: %v", evt.TicketID, err)
return
}

pushed, err := h.store.RecalculateCounterETAs(ctx, evt.CounterID)
if err != nil {
log.Printf("staff: RESTORE_STUDENT ticket=%s: eta recalc failed: %v", evt.TicketID, err)
return
}
log.Printf("staff: RESTORE_STUDENT ticket=%s: recalculated ETAs for %d waiting users at counter=%s", evt.TicketID, len(pushed), evt.CounterID)
}

// OnCallNext: not in the original checklist, added because the ticker
// needs a processing start time set somewhere, marks a ticket PROCESSING
// and starts its stuck-timer clock.
func (h *Handler) OnCallNext(ctx context.Context, evt models.StudentActionEvent) {
if err := h.store.SetTicketProcessing(ctx, evt.TicketID, evt.CounterID, time.Now()); err != nil {
log.Printf("staff: CALL_NEXT ticket=%s failed: %v", evt.TicketID, err)
return
}
log.Printf("staff: CALL_NEXT ticket=%s now processing at counter=%s", evt.TicketID, evt.CounterID)
}

// OnCounterState: UPDATE_COUNTER_STATUS, PAUSED stops routing new
// students (Express side); OPEN resumes routing and recalculates ETAs
// for anyone left waiting during the pause.
func (h *Handler) OnCounterState(ctx context.Context, evt models.CounterStatusEvent) {
if err := h.store.SetCounterStatus(ctx, evt.CounterID, evt.Status); err != nil {
log.Printf("staff: UPDATE_COUNTER_STATUS counter=%s -> %s failed: %v", evt.CounterID, evt.Status, err)
return
}
log.Printf("staff: counter=%s status set to %s", evt.CounterID, evt.Status)

if evt.Status != models.CounterOpen {
return
}
pushed, err := h.store.RecalculateCounterETAs(ctx, evt.CounterID)
if err != nil {
log.Printf("staff: UPDATE_COUNTER_STATUS counter=%s: eta recalc on reopen failed: %v", evt.CounterID, err)
return
}
log.Printf("staff: counter=%s reopened: recalculated ETAs for %d waiting users", evt.CounterID, len(pushed))
}
