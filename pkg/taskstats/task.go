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

package taskstats

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
)

// Task is an optional lifecycle handle. A nil handle is safe to use.
// It retains only its recorder and immutable event snapshot.
type Task struct {
	mu       sync.Mutex
	recorder Recorder
	event    Event
}

// Begin creates and submits a task. The disabled path allocates nothing and
// reads no clock. A task captures its recorder through the terminal event.
func Begin(observer Observer, meta Meta) *Task {
	if observer == nil {
		return nil
	}
	return BeginAt(observer, meta, time.Now())
}

// BeginAt allows an executor to retain the actual submission time while deciding
// the execution route before activating observation.
func BeginAt(observer Observer, meta Meta, submitted time.Time) (task *Task) {
	if observer == nil {
		return nil
	}
	factoryReturned := false
	defer func() {
		if !factoryReturned {
			_ = recover()
			task = nil
			warnObserverPanic("NewTask")
		}
	}()
	recorder := observer.NewTask(meta)
	factoryReturned = true
	if recorder == nil {
		return nil
	}
	task = &Task{recorder: recorder, event: Event{Version: 1, State: StatePending, Submitted: submitted}}
	task.publish(task.event)
	return task
}

func (t *Task) publish(event Event) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			warnObserverPanic("Observe")
		}
	}()
	t.recorder.Observe(event)
	returned = true
}

// Start reports execution beginning. Repeated or late calls are ignored.
func (t *Task) Start() {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.event.State != StatePending {
		t.mu.Unlock()
		return
	}
	t.event.Version++
	t.event.State = StateRunning
	t.event.Started = time.Now()
	event := t.event
	t.mu.Unlock()
	t.publish(event)
}

// CallerReturned records caller deadline/cancellation without ending execution.
func (t *Task) CallerReturned(timedOut, canceled bool) {
	if t == nil || (!timedOut && !canceled) {
		return
	}
	t.mu.Lock()
	if t.event.State == StateFinished || ((!timedOut || t.event.CallerTimedOut) && (!canceled || t.event.CallerCanceled)) {
		t.mu.Unlock()
		return
	}
	t.event.Version++
	t.event.CallerTimedOut = t.event.CallerTimedOut || timedOut
	t.event.CallerCanceled = t.event.CallerCanceled || canceled
	event := t.event
	t.mu.Unlock()
	t.publish(event)
}

// Reject reports an explicit failed admission. It is valid only before Start;
// the executor must ensure the rejected function will never run. It changes
// observation only and does not implement a queue or an admission policy.
func (t *Task) Reject() {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.event.State != StatePending {
		t.mu.Unlock()
		return
	}
	t.event.Version++
	t.event.State = StateFinished
	t.event.Finished = time.Now()
	t.event.Outcome = OutcomeRejected
	event := t.event
	t.mu.Unlock()
	t.publish(event)
}

// Finish reports actual function exit.
func (t *Task) Finish(outcome Outcome) {
	if t == nil {
		return
	}
	t.FinishAt(outcome, time.Now())
}

// FinishAt preserves actual exit time when caller completion must be reconciled
// before publishing the final snapshot.
func (t *Task) FinishAt(outcome Outcome, finished time.Time) {
	if t == nil {
		return
	}
	t.mu.Lock()
	if t.event.State == StateFinished {
		t.mu.Unlock()
		return
	}
	t.event.Version++
	t.event.State = StateFinished
	t.event.Finished = finished
	t.event.Outcome = outcome
	event := t.event
	t.mu.Unlock()
	t.publish(event)
}

// Run observes a function while preserving its panic value and Goexit behavior.
func (t *Task) Run(f func()) {
	if t == nil {
		f()
		return
	}
	t.RunOutcome(func() Outcome { f(); return OutcomeSuccess })
}

// RunOutcome observes a function that can report an execution outcome.
// The inner recovery distinguishes panic(nil) on Go 1.20 from runtime.Goexit:
// recovering a panic returns here; Goexit continues running the outer defers.
func (t *Task) RunOutcome(f func() Outcome) (outcome Outcome) {
	if t == nil {
		return f()
	}
	t.Start()
	returned, recovered := false, false
	var panicValue interface{}
	defer func() {
		switch {
		case returned:
			t.Finish(outcome)
		case recovered:
			t.Finish(OutcomePanic)
			panic(panicValue)
		default:
			t.Finish(OutcomeAborted)
		}
	}()
	func() {
		defer func() {
			panicValue = recover()
			if panicValue != nil {
				// Re-panic before unwinding this function so the caller's
				// existing panic diagnostics retain the original business stack.
				t.Finish(OutcomePanic)
				panic(panicValue)
			}
		}()
		outcome = f()
		returned = true
	}()
	recovered = true
	return outcome
}

var lastObserverPanic atomic.Int64

// A broken extension must not break task execution, but silently losing metrics
// would make failures difficult to diagnose. Bound logs globally and omit
// observer panic values and task metadata.
func warnObserverPanic(phase string) {
	defer func() { _ = recover() }()
	now := time.Now().UnixNano()
	previous := lastObserverPanic.Load()
	if previous != 0 && now-previous < int64(time.Minute) {
		return
	}
	if lastObserverPanic.CompareAndSwap(previous, now) {
		klog.Warnf("KITEX: panic in task observer %s; observation may be incomplete", phase)
	}
}
