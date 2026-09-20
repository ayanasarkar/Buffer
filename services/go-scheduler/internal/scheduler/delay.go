package scheduler

import (
	"sync"
	"time"

	"go-scheduler/internal/models"
)

// DelayTracker implements Option B: it never touches slot assignments.
// It tracks how far behind schedule the queue currently is, and lets
// callers compute a live ETA for any user's original slot on demand.
//
// ASSUMPTION: this models ONE shared delay for the whole queue, not
// per-counter delay. Your README describes a "Multi-Counter Workflow
// Pipeline" -- if counters run late independently, this needs to become
// a map[counterID]*DelayTracker instead. Confirm with the team.
type DelayTracker struct {
	mu            sync.RWMutex
	currentOffset time.Duration
}

func NewDelayTracker() *DelayTracker {
	return &DelayTracker{}
}

// RecordActualCompletion compares actual finish time against the slot's
// scheduled end. If the overrun is bigger than the delay already being
// tracked, the queue's live offset increases. An early or on-time finish
// never decreases the offset -- it doesn't "pay back" prior lateness.
func (d *DelayTracker) RecordActualCompletion(originalSlot *models.Slot, actualEnd time.Time) {
	if originalSlot == nil {
		return
	}
	overrun := actualEnd.Sub(originalSlot.EndTime)
	if overrun <= 0 {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if overrun > d.currentOffset {
		d.currentOffset = overrun
	}
}

func (d *DelayTracker) CurrentOffset() time.Duration {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.currentOffset
}

// ETAFor computes a live ETA from the original slot plus current delay,
// without mutating the slot -- so feasibility/swap logic stays untouched.
func (d *DelayTracker) ETAFor(userID string, slot *models.Slot) models.ETAUpdate {
	offset := d.CurrentOffset()
	return models.ETAUpdate{
		UserID:       userID,
		OriginalSlot: slot,
		UpdatedETA:   slot.StartTime.Add(offset),
		DelayMinutes: int(offset.Minutes()),
	}
}
