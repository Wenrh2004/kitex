/*
 * Copyright 2026 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package connstats provides optional, vendor-neutral observation of established
// logical client transports, including draining transports until logical close.
// It does not describe operating system file descriptors.
package connstats

import "time"

// State is the current logical state of an established transport.
type State uint8

const (
	// StateUnknown is never counted as an established transport.
	StateUnknown State = iota
	// StateReachable accepts new streams.
	StateReachable
	// StateDraining rejects new streams and may still serve existing streams.
	StateDraining
	// StateClosed is terminal logical close, not a statement about an OS FD.
	StateClosed
)

// Mode describes the transport's connection reuse policy.
type Mode uint8

const (
	ModeLong Mode = iota
	ModeShort
)

// Meta contains immutable metadata copied when the transport is established.
// It must not contain a transport or an RPC context.
type Meta struct {
	Service string // Configured destination service.
	Target  string // Dial target, which may be a proxy rather than a backend.
	Mode    Mode
	PoolID  uint64 // Process-local owner identity; not a default metric label.
}

// Event describes an absolute state, not an increment or decrement.
type Event struct {
	Version uint64 // Starts at 1 internally; the first delivered event may be later.
	State   State
	// Optional detailed metrics are absolute and survive skipped or reordered
	// deliveries, including a first delivery that is already Closed. Times use
	// the process clock with its monotonic component for elapsed durations.
	EstablishedAt  time.Time
	DrainingAt     time.Time
	ClosedAt       time.Time
	GoAwayReceived uint64
	CloseReason    Reason
	Streams        StreamEvent
}

// Reason is a bounded diagnostic category. Arbitrary error text is not a label.
type Reason uint8

const (
	ReasonNone Reason = iota
	ReasonCanceled
	ReasonDeadline
	ReasonNetwork
	ReasonProtocol
	ReasonOther
	ReasonGoAway
	ReasonLocal
)

// MetricsObserver optionally enables timestamps and detailed stream tracking.
// An Observer without this capability keeps the minimal state-only fast path.
// This is a marker capability: core checks its presence without invoking the
// method, including while transport construction holds a pool lock.
type MetricsObserver interface {
	ConnectionMetricsEnabled()
}

// StreamEvent is an absolute stream snapshot with its own version sequence.
// Active means registered streams whose terminal cleanup has not begun;
// Waiting means calls currently waiting for the peer's stream quota. Capacity
// is the peer limit (the initial transport default before peer SETTINGS).
// Terminal snapshots have zero gauges and contain final cumulative waits.
// QuotaWaitCount counts completed waits. Duration ends when quota waiting ends,
// or at logical transport close if the transport terminates the wait.
// Cumulative duration saturates at time.Duration's maximum instead of wrapping.
type StreamEvent struct {
	Version           uint64
	Terminal          bool
	Active            uint64
	Waiting           uint64
	Capacity          uint64
	QuotaWaitCount    uint64
	QuotaWaitDuration time.Duration
}

// MetricsRecorder is an optional capability on the Recorder returned by an
// enabled MetricsObserver. Calls are synchronous, outside transport/pool locks,
// may be concurrent and out of order, and must return promptly. Ignore stale
// versions and never revive terminal gauges. Event.Streams also carries the
// latest snapshot, especially at first activation and terminal close.
type MetricsRecorder interface {
	ObserveStreams(StreamEvent)
}

type DialOutcome uint8

const (
	DialEstablished DialOutcome = iota
	DialFailed
	DialTLSFailed
	DialTransportFailed
)

// DialEvent describes one completed real connection attempt, including TLS and
// client transport initialization. It does not count pool hits, new streams or
// describe an in-flight gauge. Duration includes all establishment stages.
type DialEvent struct {
	Duration time.Duration
	Outcome  DialOutcome
	Reason   Reason
}

// DialObserver is an optional Observer capability. ObserveDial is called once
// for each completed attempt, outside pool/transport locks, and must return
// promptly without performing network export or recursively invoking the pool.
type DialObserver interface {
	ObserveDial(Meta, DialEvent)
}

// Observer creates a recorder at most once per successfully constructed
// transport. NewConnection must return promptly and may return nil to disable
// observation of this transport. It must not count a connection until Observe:
// the first observed state may already be StateClosed. Observation calls are
// synchronous; do not export over the network or recursively invoke the pool.
type Observer interface {
	NewConnection(Meta) Recorder
}

// Recorder receives absolute states. Calls may be concurrent and arrive out of
// version order. Implementations must ignore older or duplicate versions and
// never revive a closed recorder. Observe must return promptly and must not
// retain RPC contexts or transport objects.
type Recorder interface {
	Observe(Event)
}
