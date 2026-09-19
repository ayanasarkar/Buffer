package models

import "time"

// UserRegistrationEvent is the payload published by the Express gateway
// to RabbitMQ when a user registers for a workflow.
//
// IMPORTANT: field names / JSON keys must exactly match what the Express
// service (Souvik) actually publishes. Confirm this contract with him —
// these are reasonable assumptions based on the Buffer algorithm spec.
type UserRegistrationEvent struct {
UserID            string    `json:"user_id"`
Name              string    `json:"name"`
RegisteredAt      time.Time `json:"registered_at"`
Age               int       `json:"age"`                 // used for Age Factor (A_i)
TravelTimeMinutes int       `json:"travel_time_minutes"` // time to travel home after appointment
DeadlineTime      time.Time `json:"deadline_time"`       // must be home by this time
ConvenienceScore  float64   `json:"convenience_score"`   // 0.0-1.0 raw input for Convenience Factor (C_i)
WorkflowStage     string    `json:"workflow_stage"`      // e.g. "registration", "fee_payment"
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
