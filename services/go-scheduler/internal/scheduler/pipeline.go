package scheduler

import (
	"log"
	"sync"

	"go-scheduler/internal/models"
)

// Pipeline implements Processor. It owns the day slot pool and turns
// incoming events (batch or single FCFS) into final ScheduledUser results,
// applying cutoff, assignment, feasibility, and swap logic in order.
type Pipeline struct {
	Schedule DaySchedule
	Weights  Weights
	Out      chan<- models.ScheduledUser

	// mu serializes all slot pool access. Batch timers and the FCFS path run
	// on different goroutines and both mutate slots.
	mu    sync.Mutex
	slots []*models.Slot
}

func NewPipeline(schedule DaySchedule, weights Weights, out chan<- models.ScheduledUser) *Pipeline {
	return &Pipeline{
		Schedule: schedule,
		Weights:  weights,
		Out:      out,
		slots:    GenerateSlots(schedule),
	}
}

// WeightsFor returns the fairness weights for a batch.
//
// ASSUMPTION: fairness.go has no separate travel term (F = w_A*A + w_C*C), so
// this treats the Convenience factor as the travel-derived part and zeroes it
// for Phase 2 (travel factor EXCLUDED). If travel is a separate term in the
// real formula, this is the only function to change.
func WeightsFor(base Weights, includeTravel bool) Weights {
	if !includeTravel {
		base.Convenience = 0
	}
	return base
}

// ProcessBufferBatch scores a batch with the pipeline default weights.
func (p *Pipeline) ProcessBufferBatch(batch []models.UserRegistrationEvent) {
	p.ProcessPhaseBatch(batch, p.Weights)
}

// ProcessPhaseBatch runs cutoff, fairness sort with weights w, slot
// assignment, then feasibility and swap resolution across the whole batch.
func (p *Pipeline) ProcessPhaseBatch(batch []models.UserRegistrationEvent, w Weights) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var eligible []models.UserRegistrationEvent

	for _, evt := range batch {
		if result, cutoff := ApplyCutoff(evt, p.Schedule.Close); cutoff {
			log.Printf("scheduler: user_id=%s unscheduled (cutoff)", evt.UserID)
			p.Out <- result
			continue
		}
		eligible = append(eligible, evt)
	}

	scored := ScoreAndSortBatch(eligible, w)
	results := AssignBufferBatch(scored, p.slots)

	candidates := make([]*SwapCandidate, 0, len(results))
	for i := range results {
		if results[i].AssignedSlot == nil {
			continue // already unscheduled (ran out of slots)
		}
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

// ProcessFCFSUser handles one instant registrant: cutoff check, earliest
// free slot, then a solo feasibility check (no swap pool for a single user).
func (p *Pipeline) ProcessFCFSUser(evt models.UserRegistrationEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()

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
