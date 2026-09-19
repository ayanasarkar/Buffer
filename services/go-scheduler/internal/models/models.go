package models

import "time"

// UserRegistrationEvent is the payload published by the Express gateway
// to RabbitMQ when a user registers for a workflow.
//
// IMPORTANT: field names / JSON keys must exactly match what the Express
// service (Souvik) actually publishes. Confirm this contract with him —
// these are reasonable assumptions based on the Buffer algorithm spec.
type UserRegistrationEvent struct {
	AlgorithmPhase    AlgorithmPhase `json:"algorithmPhase"`
	UserID            string         `json:"user_id"`
	Name              string         `json:"name"`
	RegisteredAt      time.Time      `json:"registered_at"`
	Age               int            `json:"age"`                 // used for Age Factor (A_i)
	TravelTimeMinutes int            `json:"travel_time_minutes"` // time to travel home after appointment
	DeadlineTime      time.Time      `json:"deadline_time"`       // must be home by this time
	ConvenienceScore  float64        `json:"convenience_score"`   // 0.0-1.0 raw input for Convenience Factor (C_i)
	WorkflowStage     string         `json:"workflow_stage"`      // e.g. "registration", "fee_payment"
}

// Group is which scheduling bucket a user falls into.
type Group string

const (
	GroupBuffer      Group = "BUFFER"
	GroupFCFS        Group = "FCFS"
	GroupUnscheduled Group = "UNSCHEDULED"
)

// Slot is a single bookable appointment slot for the day.
type Slot struct {
	ID        string
	StartTime time.Time
	EndTime   time.Time
	Assigned  bool
	UserID    string
}

// ScheduledUser is the final row written to Postgres for each user.
type ScheduledUser struct {
	UserID            string
	Name              string
	Group             Group
	FairnessScore     float64
	AssignedSlot      *Slot
	Unscheduled       bool
	UnscheduledReason string
	SwappedWithUserID string // non-empty if this user's slot was swapped
	SwapReason        string
}

// --- Live delay / ETA tracking (Option B) ---
// These support real-time ETA updates without reshuffling the original
// assigned slots: if a user overruns, we track the resulting delay and
// recompute a live ETA for everyone still waiting.

// StageCompletionEvent represents a user actually finishing at a
// counter, at some real time that may be later than their originally
// assigned slot end time.
//
// ASSUMPTION: this event type doesn't exist yet in the Express->Go
// queue contract -- see the note after these commands.
type StageCompletionEvent struct {
	UserID    string    `json:"user_id"`
	ActualEnd time.Time `json:"actual_end"`
}

// ETAUpdate is a user's live, recalculated ETA -- their AssignedSlot
// is untouched; this is purely informational for the frontend.
type ETAUpdate struct {
	UserID       string
	OriginalSlot *Slot
	UpdatedETA   time.Time
	DelayMinutes int
}
