package models

// AlgorithmPhase is the tag Express puts on each ticket before publishing.
type AlgorithmPhase string

const (
	PhaseOne  AlgorithmPhase = "PHASE_1" // registered > 1h before start, travel factor INCLUDED
	PhaseTwo  AlgorithmPhase = "PHASE_2" // registered 1h to 10m before start, travel factor EXCLUDED
	PhaseFCFS AlgorithmPhase = "FCFS"    // < 10m before start or active queue, instant
)
