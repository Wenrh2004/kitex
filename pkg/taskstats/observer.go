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

// Package taskstats provides optional vendor-neutral task lifecycle observation.
// Accepted tasks are counted until their function actually exits, even when an RPC caller
// has already timed out. It does not describe all goroutines or OS threads.
package taskstats

import "time"

// State is an absolute task lifecycle state.
type State uint8

const (
	StateUnknown State = iota
	StatePending
	StateRunning
	StateFinished
)

// Outcome describes actual execution, independently of the caller's timeout.
type Outcome uint8

const (
	OutcomeUnknown Outcome = iota
	OutcomeSuccess
	OutcomeError
	OutcomePanic
	// OutcomeAborted reports runtime.Goexit rather than a business panic.
	OutcomeAborted
	// OutcomeRejected is an explicit failed admission before execution starts.
	OutcomeRejected
)

// Meta is immutable and must not retain contexts, tasks or request objects.
// Service must be empty for a shared executor's worker/configuration metrics.
type Meta struct {
	Executor string
	Type     string
	Role     string
	Service  string
}

// Event contains an absolute, complete snapshot. Times use the process clock;
// their monotonic component should be preserved when computing durations.
type Event struct {
	Version        uint64
	State          State
	Submitted      time.Time
	Started        time.Time
	Finished       time.Time
	Outcome        Outcome
	CallerTimedOut bool
	CallerCanceled bool
}

// Observer is optional and disabled by default. NewTask must return promptly and
// may return nil to disable a task. Observation must not alter execution policy.
type Observer interface{ NewTask(Meta) Recorder }

// Recorder receives concurrent, potentially out-of-order snapshots outside core
// locks. Ignore duplicate/older versions and never revive a finished task.
// Observe must return promptly. Observer panics are isolated from execution.
type Recorder interface{ Observe(Event) }

// PoolSnapshot reports only facts available through safe public capabilities.
// Unknown values must be omitted by adapters rather than reported as zero.
// MaxIdle is a retention setting, never a hard execution or goroutine cap.
type PoolSnapshot struct {
	ActiveWorkers      int64
	ActiveWorkersKnown bool
	IdleWorkers        int64
	IdleWorkersKnown   bool
	Queued             int64
	QueuedKnown        bool
	// WorkerCapacity may be reported only for an actual hard worker bound.
	// Idle retention settings or soft autoscaling targets are not capacity.
	WorkerCapacity      int64
	WorkerCapacityKnown bool
	QueueRemaining      int64
	QueueRemainingKnown bool
	Workers             int64
	WorkersKnown        bool
	QueueCapacity       int64
	QueueCapacityKnown  bool
	MaxIdle             int64
	MaxIdleKnown        bool
	MaxIdleTime         time.Duration
	MaxIdleTimeKnown    bool
}
