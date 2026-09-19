package scheduler

import (
"log"

"go-scheduler/internal/models"
)

// Pipeline implements Processor. It owns the day's slot pool and turns
// incoming events (buffered batch or single FCFS) into final
// ScheduledUser results, applying cutoff, assignment, feasibility, and
// swap logic in order.
type Pipeline struct {
Schedule DaySchedule
Weights  Weights
Out      chan<- models.ScheduledUser

slots []*models.Slot
}

// NewPipeline generates the day's slots up front and returns a ready-to-use
// Pipeline. out is where finished ScheduledUser results are sent -- the
// caller (main.go) is responsible for reading from it, e.g. to persist to
// Postgres.
func NewPipeline(schedule DaySchedule, weights Weights, out chan<- models.ScheduledUser) *Pipeline {
return &Pipeline{
Schedule: schedule,
Weights:  weights,
Out:      out,
slots:    GenerateSlots(schedule),
}
}

// ProcessBufferBatch handles the full Buffer Group batch once the 60-minute
// window closes: cutoff check, fairness scoring + sort, slot assignment,
// then feasibility + swap resolution across the whole batch at once (so
// swaps can happen between any two Buffer users).
func (p *Pipeline) ProcessBufferBatch(batch []models.UserRegistrationEvent) {
var eligible []models.UserRegistrationEvent

for _, evt := range batch {
if result, cutoff := ApplyCutoff(evt, p.Schedule.Close); cutoff {
log.Printf("scheduler: user_id=%s unscheduled (cutoff)", evt.UserID)
p.Out <- result
continue
}
eligible = append(eligible, evt)
}

scored := ScoreAndSortBatch(eligible, p.Weights)
results := AssignBufferBatch(scored, p.slots)

candidates := make([]*SwapCandidate, 0, len(results))
for i := range results {
if results[i].AssignedSlot == nil {
continue // already unscheduled (ran out of slots)
}
// find matching event for this result to get travel/deadline info
for _, su := range scored {
if su.Event.UserID == results[i].UserID {
candidates = append(candidates, &SwapCandidate{
Result: &results[i],
Event:  su.Event,
})
break
}
}
}

ResolveFeasibility(candidates)

for i := range results {
log.Printf("scheduler: user_id=%s group=%s unscheduled=%v",
results[i].UserID, results[i].Group, results[i].Unscheduled)
p.Out <- results[i]
}
}

// ProcessFCFSUser handles a single registrant who arrives after the buffer
// window has closed: cutoff check, immediate earliest-slot assignment,
// then a solo feasibility check (no swap pool -- FCFS users are processed
// one at a time as they stream in, so there's no batch of peers to swap
// with yet).
//
// ASSUMPTION: FCFS users are not swap candidates against each other or
// against Buffer users after the fact, since they arrive one at a time
// with no batch context. If your algorithm doc wants FCFS users to also
// participate in swaps against later arrivals, that needs a shared,
// mutex-protected candidate pool across calls -- flag this if so, it's a
// bigger structural change.
func (p *Pipeline) ProcessFCFSUser(evt models.UserRegistrationEvent) {
if result, cutoff := ApplyCutoff(evt, p.Schedule.Close); cutoff {
log.Printf("scheduler: user_id=%s unscheduled (cutoff)", evt.UserID)
p.Out <- result
return
}

result := AssignFCFSUser(evt, p.slots)

if result.AssignedSlot != nil && !IsFeasible(result.AssignedSlot, evt) {
result.AssignedSlot.Assigned = false
result.AssignedSlot.UserID = ""
result.AssignedSlot = nil
result.Group = models.GroupUnscheduled
result.Unscheduled = true
result.UnscheduledReason = "travel deadline infeasible (FCFS, no swap pool)"
}

log.Printf("scheduler: user_id=%s group=%s unscheduled=%v",
result.UserID, result.Group, result.Unscheduled)
p.Out <- result
}
