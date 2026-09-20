package scheduler

import (
	"sort"
	"time"

	"go-scheduler/internal/models"
)

// Weights holds the w_A / w_C coefficients from F_i = w_A*A_i + w_C*C_i.
//
// ASSUMPTION: 0.5/0.5 is a placeholder split. Replace with the real
// weights from your algorithm doc -- this single struct is the only
// place they're defined, so it's a one-line fix once you have them.
type Weights struct {
	Age         float64 // w_A
	Convenience float64 // w_C
}

var DefaultWeights = Weights{
	Age:         0.5,
	Convenience: 0.5,
}

// ScoredUser pairs a raw registration event with its computed fairness
// score, ready for sorting.
type ScoredUser struct {
	Event         models.UserRegistrationEvent
	FairnessScore float64
}

// AgeFactor normalizes a raw age into A_i in [0, 1].
//
// ASSUMPTION: older = higher priority, normalized against a fixed max age
// of 100. Confirm this direction and cutoff match your doc -- e.g. some
// systems weight *both* very young and very old higher (accessibility
// priority) rather than a straight linear ramp.
func AgeFactor(age int) float64 {
	const maxAge = 100.0
	if age <= 0 {
		return 0
	}
	if float64(age) >= maxAge {
		return 1.0
	}
	return float64(age) / maxAge
}

// ConvenienceFactor normalizes the raw convenience input into C_i in [0, 1].
//
// ASSUMPTION: models.UserRegistrationEvent.ConvenienceScore already arrives
// pre-normalized to [0,1] from Express (e.g. based on distance/transport
// mode). If it doesn't, clamp/scale it here instead.
func ConvenienceFactor(raw float64) float64 {
	switch {
	case raw < 0:
		return 0
	case raw > 1:
		return 1
	default:
		return raw
	}
}

// CalculateFairnessScore computes F_i = w_A*A_i + w_C*C_i for one user.
func CalculateFairnessScore(evt models.UserRegistrationEvent, w Weights) float64 {
	a := AgeFactor(evt.Age)
	c := ConvenienceFactor(evt.ConvenienceScore)
	return w.Age*a + w.Convenience*c
}

// ScoreAndSortBatch scores every user in the batch, then sorts by:
//  1. highest fairness score first
//  2. earliest registration time (tie-break)
//  3. lowest user ID, lexically (final tie-break)
//
// This ordering matches "Sort this batch of users first by highest
// Fairness Score, then by earliest registration time, then by User ID"
// from the spec.
func ScoreAndSortBatch(batch []models.UserRegistrationEvent, w Weights) []ScoredUser {
	scored := make([]ScoredUser, len(batch))
	for i, evt := range batch {
		scored[i] = ScoredUser{
			Event:         evt,
			FairnessScore: CalculateFairnessScore(evt, w),
		}
	}

	sort.Slice(scored, func(i, j int) bool {
		si, sj := scored[i], scored[j]

		if si.FairnessScore != sj.FairnessScore {
			return si.FairnessScore > sj.FairnessScore // higher score first
		}
		if !si.Event.RegisteredAt.Equal(sj.Event.RegisteredAt) {
			return si.Event.RegisteredAt.Before(sj.Event.RegisteredAt) // earlier first
		}
		return si.Event.UserID < sj.Event.UserID // lexical tie-break
	})

	return scored
}

// zeroTime is a small helper used by tests / callers that need a sentinel;
// kept here to avoid importing time in files that don't otherwise need it.
var zeroTime = time.Time{}
