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
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
)

type observerFunc func(Meta) Recorder

func (f observerFunc) NewTask(m Meta) Recorder { return f(m) }

type recorderFunc func(Event)

func (f recorderFunc) Observe(e Event) { f(e) }

func TestTaskConcurrentTerminalAndReordering(t *testing.T) {
	var mu sync.Mutex
	var events []Event
	starting := make(chan struct{})
	release := make(chan struct{})
	o := observerFunc(func(Meta) Recorder {
		return recorderFunc(func(e Event) {
			if e.State == StateRunning && e.Version == 2 {
				close(starting)
				<-release
			}
			mu.Lock()
			events = append(events, e)
			mu.Unlock()
		})
	})
	task := Begin(o, Meta{})
	done := make(chan struct{})
	go func() { task.Start(); close(done) }()
	<-starting
	task.CallerReturned(true, false)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); task.Finish(OutcomeSuccess) }()
	}
	wg.Wait()
	close(release)
	<-done
	task.Start()
	task.CallerReturned(false, true)
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 4 {
		t.Fatalf("events %v", events)
	}
	// Start's blocked callback arrives after the absolute terminal event.
	final := events[2]
	if final.State != StateFinished || final.Version != 4 || !final.CallerTimedOut || final.Outcome != OutcomeSuccess {
		t.Fatalf("final snapshot %+v", final)
	}
	if final.Started.Before(final.Submitted) || final.Finished.Before(final.Started) {
		t.Fatal("invalid actual times")
	}
	if events[3].Version != 2 {
		t.Fatal("expected reordered start callback")
	}
}

func TestObserverFailuresAndBusinessPanic(t *testing.T) {
	factoryPanic := observerFunc(func(Meta) Recorder { panic("factory") })
	if Begin(factoryPanic, Meta{}) != nil {
		t.Fatal("failed factory must disable task")
	}
	task := Begin(observerFunc(func(Meta) Recorder { return recorderFunc(func(Event) { panic("observer") }) }), Meta{})
	ran := false
	task.Run(func() { ran = true })
	if !ran {
		t.Fatal("observer panic changed execution")
	}
	var final Event
	task = Begin(observerFunc(func(Meta) Recorder { return recorderFunc(func(e Event) { final = e }) }), Meta{})
	func() {
		defer func() {
			if recover() != "business" {
				t.Error("business panic changed")
			}
		}()
		task.Run(func() { panic("business") })
	}()
	if final.State != StateFinished || final.Outcome != OutcomePanic {
		t.Fatalf("terminal %+v", final)
	}
	var disabled *Task
	disabled.Start()
	disabled.CallerReturned(true, true)
	disabled.Finish(OutcomeSuccess)
	disabled.Run(func() { ran = true })
}

func TestFinishAtRetainsActualExit(t *testing.T) {
	var final Event
	submitted := time.Now().Add(-time.Second)
	task := BeginAt(observerFunc(func(Meta) Recorder { return recorderFunc(func(e Event) { final = e }) }), Meta{}, submitted)
	task.Start()
	finished := time.Now()
	task.CallerReturned(false, true)
	task.FinishAt(OutcomeError, finished)
	if final.Submitted != submitted || final.Finished != finished || !final.CallerCanceled {
		t.Fatalf("%+v", final)
	}
}

func BenchmarkTaskObservationDisabled(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		task := Begin(nil, Meta{})
		task.Start()
		task.Finish(OutcomeSuccess)
	}
}

func TestRunPanicNilAndGoexit(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    func()
		want Outcome
	}{{"panic_nil", func() { panic(nil) }, OutcomePanic}, {"goexit", runtime.Goexit, OutcomeAborted}} {
		t.Run(tc.name, func(t *testing.T) {
			events := make(chan Event, 3)
			task := Begin(observerFunc(func(Meta) Recorder { return recorderFunc(func(e Event) { events <- e }) }), Meta{})
			done := make(chan struct{})
			returned := make(chan struct{}, 1)
			go func() { defer close(done); defer func() { _ = recover() }(); task.Run(tc.f); returned <- struct{}{} }()
			<-done
			<-events
			<-events
			final := <-events
			if final.Outcome != tc.want || final.State != StateFinished {
				t.Fatalf("%+v", final)
			}
			select {
			case <-returned:
				t.Fatal("panic/Goexit incorrectly returned")
			default:
			}
		})
	}
}

func TestCallerFlagsAreMonotonicAndIdempotent(t *testing.T) {
	var events []Event
	task := Begin(observerFunc(func(Meta) Recorder { return recorderFunc(func(e Event) { events = append(events, e) }) }), Meta{})
	task.CallerReturned(true, false)
	task.CallerReturned(false, true)
	task.CallerReturned(true, false)
	task.CallerReturned(false, true)
	task.CallerReturned(true, true)
	task.Finish(OutcomeError)
	if len(events) != 4 || events[3].Version != 4 || !events[3].CallerTimedOut || !events[3].CallerCanceled {
		t.Fatalf("%+v", events)
	}
}

func TestExplicitAdmissionRejectOnlyPending(t *testing.T) {
	var events []Event
	o := observerFunc(func(Meta) Recorder { return recorderFunc(func(e Event) { events = append(events, e) }) })
	task := Begin(o, Meta{})
	task.Reject()
	task.Reject()
	task.Start()
	task.Finish(OutcomeSuccess)
	if len(events) != 2 || !events[1].Started.IsZero() || events[1].Outcome != OutcomeRejected {
		t.Fatalf("%+v", events)
	}
	events = nil
	task = Begin(o, Meta{})
	task.Start()
	task.Reject()
	task.Finish(OutcomeSuccess)
	if len(events) != 3 || events[2].Outcome != OutcomeSuccess {
		t.Fatalf("running task changed by reject %+v", events)
	}
}

func taskBusinessPanic() { panic("original") }

func TestRunPreservesBusinessPanicStack(t *testing.T) {
	task := Begin(observerFunc(func(Meta) Recorder { return recorderFunc(func(Event) {}) }), Meta{})
	defer func() {
		if recover() != "original" {
			t.Error("panic value changed")
		}
		if !strings.Contains(string(debug.Stack()), "taskBusinessPanic") {
			t.Error("business stack lost")
		}
	}()
	task.Run(taskBusinessPanic)
}
