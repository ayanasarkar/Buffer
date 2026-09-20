package scheduler

import (
	"time"

	"go-scheduler/internal/models"
)

// CutoffDuration is how close to closing time a registration can arrive
// before it's automatically marked UNSCHEDULED, per the spec: "Registrations
// within 2 hours of queue closing are marked UNSCHEDULED."
const CutoffDuration = 2 * time.Hour

// IsPastCutoff reports whether registeredAt falls within CutoffDuration of
// closeTime -- i.e. too late to be scheduled at all.
func IsPastCutoff(registeredAt, closeTime time.Time) bool {
	return !registeredAt.Before(closeTime.Add(-CutoffDuration))
}

// ApplyCutoff checks a single event against the cutoff rule. If it's past
// cutoff, it returns a ready-made UNSCHEDULED result and ok=true, meaning
// the caller should stop here and not attempt slot assignment. If ok is
// false, the event is still eligible and the caller should proceed
// normally (buffer batching or FCFS assignment).
func ApplyCutoff(evt models.UserRegistrationEvent, closeTime time.Time) (result models.ScheduledUser, ok bool) {
	if IsPastCutoff(evt.RegisteredAt, closeTime) {
		return models.ScheduledUser{
			UserID:            evt.UserID,
			Name:              evt.Name,
			Group:             models.GroupUnscheduled,
			Unscheduled:       true,
			UnscheduledReason: "registered within 2 hours of closing (cutoff)",
		}, true
	}
	return models.ScheduledUser{}, false
}

// ReturnHomeTime computes when a user would get home after their
// appointment: appointment_end + travel_time_back.
func ReturnHomeTime(slot *models.Slot, travelTimeMinutes int) time.Time {
	return slot.EndTime.Add(time.Duration(travelTimeMinutes) * time.Minute)
}

// IsFeasible reports whether a user assigned to slot can still make it
// home by their deadline.
func IsFeasible(slot *models.Slot, evt models.UserRegistrationEvent) bool {
	if slot == nil {
		return false
	}
	returnTime := ReturnHomeTime(slot, evt.TravelTimeMinutes)
	return !returnTime.After(evt.DeadlineTime)
}

// CheckFeasibility re-validates a ScheduledUser's assigned slot against
// their travel deadline. It does not mutate su; the caller decides what to
// do with an infeasible result (typically: attempt a swap, see swap.go).
func CheckFeasibility(su models.ScheduledUser, evt models.UserRegistrationEvent) bool {
	if su.AssignedSlot == nil {
		return false
	}
	return IsFeasible(su.AssignedSlot, evt)
}
