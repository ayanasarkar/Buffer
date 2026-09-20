package scheduler

import (
	"fmt"
	"time"

	"go-scheduler/internal/models"
)

// SwapCandidate pairs a ScheduledUser result with the original event data
// needed to evaluate feasibility (TravelTimeMinutes, DeadlineTime).
type SwapCandidate struct {
	Result *models.ScheduledUser
	Event  models.UserRegistrationEvent
}

// Slack returns how much spare time a user would have before their
// deadline if assigned to slot -- positive means margin to spare
// (more flexible), negative means they'd miss their deadline.
func Slack(slot *models.Slot, evt models.UserRegistrationEvent) time.Duration {
	return evt.DeadlineTime.Sub(ReturnHomeTime(slot, evt.TravelTimeMinutes))
}

// AttemptSwap tries to rescue an infeasible user by swapping slots with a
// more flexible user who holds an earlier slot.
//
// ASSUMPTION: "more flexible" is interpreted as "has the most slack (spare
// time before their own deadline) after taking over the infeasible user's
// later slot." Among all candidates where the swap leaves BOTH users
// feasible, the one with the most post-swap slack is chosen. Confirm this
// matches your algorithm doc's intended notion of flexibility -- an
// alternative reading is simply "earliest available earlier slot," which
// would need a different sort key.
func AttemptSwap(infeasible *SwapCandidate, pool []*SwapCandidate) bool {
	if infeasible.Result.AssignedSlot == nil {
		return false
	}
	mySlot := infeasible.Result.AssignedSlot

	var best *SwapCandidate
	var bestSlack time.Duration

	for _, candidate := range pool {
		if candidate.Result.UserID == infeasible.Result.UserID {
			continue
		}
		if candidate.Result.AssignedSlot == nil {
			continue
		}
		theirSlot := candidate.Result.AssignedSlot
		if !theirSlot.StartTime.Before(mySlot.StartTime) {
			continue // only consider strictly earlier slots
		}

		// would swapping make the infeasible user feasible?
		if !IsFeasible(theirSlot, infeasible.Event) {
			continue
		}
		// would the candidate still be feasible with the later slot?
		candidateSlack := Slack(mySlot, candidate.Event)
		if candidateSlack < 0 {
			continue
		}

		if best == nil || candidateSlack > bestSlack {
			best = candidate
			bestSlack = candidateSlack
		}
	}

	if best == nil {
		return false
	}

	theirSlot := best.Result.AssignedSlot

	infeasible.Result.AssignedSlot = theirSlot
	best.Result.AssignedSlot = mySlot
	theirSlot.UserID = infeasible.Result.UserID
	mySlot.UserID = best.Result.UserID

	infeasible.Result.SwappedWithUserID = best.Result.UserID
	infeasible.Result.SwapReason = fmt.Sprintf(
		"swapped into slot %s (from user %s) to meet travel deadline",
		theirSlot.ID, best.Result.UserID,
	)
	best.Result.SwappedWithUserID = infeasible.Result.UserID
	best.Result.SwapReason = fmt.Sprintf(
		"gave up slot %s to user %s (travel deadline); took slot %s instead",
		theirSlot.ID, infeasible.Result.UserID, mySlot.ID,
	)

	return true
}

// ResolveFeasibility walks every scheduled user and, for anyone whose
// assigned slot fails their travel deadline, attempts a swap. If no swap
// rescues them, they're released back to Unscheduled with a reason rather
// than silently keeping an invalid slot.
func ResolveFeasibility(candidates []*SwapCandidate) {
	for _, c := range candidates {
		if c.Result.AssignedSlot == nil {
			continue
		}
		if IsFeasible(c.Result.AssignedSlot, c.Event) {
			continue
		}
		if AttemptSwap(c, candidates) {
			continue
		}

		c.Result.AssignedSlot.Assigned = false
		c.Result.AssignedSlot.UserID = ""
		c.Result.AssignedSlot = nil
		c.Result.Group = models.GroupUnscheduled
		c.Result.Unscheduled = true
		c.Result.UnscheduledReason = "travel deadline infeasible and no valid swap found"
	}
}
