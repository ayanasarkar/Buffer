package scheduler

import (
	"fmt"
	"time"

	"go-scheduler/internal/models"
)

// DaySchedule defines the working day's bounds and any break windows to
// skip when generating slots.
type DaySchedule struct {
	Open         time.Time
	Close        time.Time
	SlotDuration time.Duration
	Breaks       []TimeRange
}

type TimeRange struct {
	Start time.Time
	End   time.Time
}

func (t TimeRange) overlaps(slotStart, slotEnd time.Time) bool {
	return slotStart.Before(t.End) && t.Start.Before(slotEnd)
}

func GenerateSlots(d DaySchedule) []*models.Slot {
	var slots []*models.Slot

	id := 0
	for start := d.Open; !start.Add(d.SlotDuration).After(d.Close); start = start.Add(d.SlotDuration) {
		end := start.Add(d.SlotDuration)

		blocked := false
		for _, br := range d.Breaks {
			if br.overlaps(start, end) {
				blocked = true
				break
			}
		}
		if blocked {
			continue
		}

		id++
		slots = append(slots, &models.Slot{
			ID:        fmt.Sprintf("slot-%04d", id),
			StartTime: start,
			EndTime:   end,
			Assigned:  false,
		})
	}

	return slots
}

func AssignBufferBatch(scored []ScoredUser, slots []*models.Slot) []models.ScheduledUser {
	results := make([]models.ScheduledUser, 0, len(scored))
	slotIdx := 0

	for _, su := range scored {
		for slotIdx < len(slots) && slots[slotIdx].Assigned {
			slotIdx++
		}

		if slotIdx >= len(slots) {
			results = append(results, models.ScheduledUser{
				UserID:            su.Event.UserID,
				Name:              su.Event.Name,
				Group:             models.GroupUnscheduled,
				FairnessScore:     su.FairnessScore,
				Unscheduled:       true,
				UnscheduledReason: "no slots remaining",
			})
			continue
		}

		slot := slots[slotIdx]
		slot.Assigned = true
		slot.UserID = su.Event.UserID
		slotIdx++

		results = append(results, models.ScheduledUser{
			UserID:        su.Event.UserID,
			Name:          su.Event.Name,
			Group:         models.GroupBuffer,
			FairnessScore: su.FairnessScore,
			AssignedSlot:  slot,
		})
	}

	return results
}

func AssignFCFSUser(evt models.UserRegistrationEvent, slots []*models.Slot) models.ScheduledUser {
	for _, slot := range slots {
		if !slot.Assigned {
			slot.Assigned = true
			slot.UserID = evt.UserID
			return models.ScheduledUser{
				UserID:       evt.UserID,
				Name:         evt.Name,
				Group:        models.GroupFCFS,
				AssignedSlot: slot,
			}
		}
	}

	return models.ScheduledUser{
		UserID:            evt.UserID,
		Name:              evt.Name,
		Group:             models.GroupUnscheduled,
		Unscheduled:       true,
		UnscheduledReason: "no slots remaining",
	}
}
