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

package taskscope

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/taskstats"
)

type observerFunc func(taskstats.Meta) taskstats.Recorder

func (f observerFunc) NewTask(meta taskstats.Meta) taskstats.Recorder { return f(meta) }

type recorderFunc func(taskstats.Event)

func (f recorderFunc) Observe(event taskstats.Event) { f(event) }

type eventsRecorder struct {
	mu     sync.Mutex
	events []taskstats.Event
}

func (r *eventsRecorder) Observe(event taskstats.Event) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *eventsRecorder) snapshot() []taskstats.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]taskstats.Event(nil), r.events...)
}

func wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not complete")
	}
}

type constructionValueKey struct{}

func TestConstructionContextDerivedObservation(t *testing.T) {
	parentValue := new(int)
	parent, cancelParent := context.WithCancelCause(context.WithValue(context.Background(), constructionValueKey{}, parentValue))
	defer cancelParent(nil)
	parentDeadline := time.Now().Add(time.Hour)
	parent, cancelDeadline := context.WithDeadline(parent, parentDeadline)
	defer cancelDeadline()
	ctx, scope := NewContext(parent)
	valueCtx := context.WithValue(ctx, constructionValueKey{}, "derived")
	cancelCtx, cancelChild := context.WithCancel(valueCtx)
	defer cancelChild()
	childDeadline := parentDeadline.Add(-time.Minute)
	deadlineCtx, cancelChildDeadline := context.WithDeadline(valueCtx, childDeadline)
	defer cancelChildDeadline()

	var factories atomic.Int32
	r := &eventsRecorder{}
	o := observerFunc(func(taskstats.Meta) taskstats.Recorder {
		factories.Add(1)
		return r
	})
	contexts := []struct {
		name     string
		ctx      context.Context
		value    interface{}
		deadline time.Time
	}{
		{"scope", ctx, parentValue, parentDeadline},
		{"value", valueCtx, "derived", parentDeadline},
		{"cancel", cancelCtx, "derived", parentDeadline},
		{"deadline", deadlineCtx, "derived", childDeadline},
	}
	for _, c := range contexts {
		if got := c.ctx.Value(constructionValueKey{}); got != c.value {
			t.Fatalf("%s parent value = %v, want %v", c.name, got, c.value)
		}
		if deadline, ok := c.ctx.Deadline(); !ok || deadline != c.deadline || c.ctx.Err() != nil {
			t.Fatalf("%s deadline = (%v, %v), error = %v", c.name, deadline, ok, c.ctx.Err())
		}
		taskstats.Begin(Observer(c.ctx, o), taskstats.Meta{Executor: c.name}).Run(func() {})
	}
	if factories.Load() != 0 || len(r.snapshot()) != 0 {
		t.Fatal("a standard derived context bypassed construction observation")
	}
	cancelChild()
	wait(t, cancelCtx.Done())
	if cancelCtx.Err() != context.Canceled || ctx.Err() != nil || deadlineCtx.Err() != nil {
		t.Fatal("child cancellation affected its parent or sibling")
	}
	cause := errors.New("parent construction canceled")
	cancelParent(cause)
	for _, c := range contexts {
		wait(t, c.ctx.Done())
		wantCause := cause
		if c.ctx == cancelCtx {
			wantCause = context.Canceled
		}
		if c.ctx.Err() != context.Canceled || context.Cause(c.ctx) != wantCause {
			t.Fatalf("%s lost cancellation: error=%v cause=%v", c.name, c.ctx.Err(), context.Cause(c.ctx))
		}
	}
	scope.Activate()
	scope.Activate()
	if events := r.snapshot(); factories.Load() != int32(len(contexts)) || len(events) != len(contexts) {
		t.Fatalf("derived tasks were lost or activated twice: factories=%d events=%+v", factories.Load(), events)
	} else {
		for _, event := range events {
			if event.State != taskstats.StateFinished || event.Outcome != taskstats.OutcomeSuccess {
				t.Fatalf("parent cancellation changed actual completion: %+v", event)
			}
		}
	}
}

func TestConstructionContextCanceledBeforeSubmission(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "canceled-parent"
		if expired {
			name = "expired-child"
		}
		t.Run(name, func(t *testing.T) {
			parent, cancelParent := context.WithCancel(context.Background())
			defer cancelParent()
			if !expired {
				cancelParent()
			}
			ctx, scope := NewContext(parent)
			wantErr := context.Canceled
			if expired {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				wantErr = context.DeadlineExceeded
			}
			wait(t, ctx.Done())
			if ctx.Err() != wantErr || context.Cause(ctx) != wantErr {
				t.Fatalf("cancellation = %v, cause = %v, want %v", ctx.Err(), context.Cause(ctx), wantErr)
			}
			r := &eventsRecorder{}
			var factories, executions atomic.Int32
			taskstats.Begin(Observer(ctx, observerFunc(func(taskstats.Meta) taskstats.Recorder {
				factories.Add(1)
				return r
			})), taskstats.Meta{}).Run(func() { executions.Add(1) })
			if factories.Load() != 0 || executions.Load() != 1 {
				t.Fatal("cancellation bypassed deferral or gated actual execution")
			}
			scope.Activate()
			if events := r.snapshot(); factories.Load() != 1 || len(events) != 1 || events[0].State != taskstats.StateFinished || events[0].Outcome != taskstats.OutcomeSuccess {
				t.Fatalf("canceled-context actual exit = %+v; factories=%d", events, factories.Load())
			}
		})
	}
}

type constructionValueContext struct{ context.Context }

func (c constructionValueContext) Value(key interface{}) interface{} {
	switch key.(type) {
	case constructionValueKey:
		return "custom parent"
	case []constructionValueKey:
		return "noncomparable parent key"
	}
	return c.Context.Value(key)
}

func TestConstructionContextCustomValuesAndNestedScopes(t *testing.T) {
	outerCtx, outer := NewContext(constructionValueContext{context.Background()})
	innerCtx, inner := NewContext(context.WithValue(outerCtx, constructionValueKey{}, "inner"))
	if outerCtx.Value(constructionValueKey{}) != "custom parent" || innerCtx.Value(constructionValueKey{}) != "inner" || innerCtx.Value([]constructionValueKey{{}}) != "noncomparable parent key" || innerCtx.Value(nil) != nil {
		t.Fatal("custom or standard parent Value lookup changed")
	}
	if _, ok := innerCtx.Deadline(); ok || innerCtx.Done() != nil || innerCtx.Err() != nil {
		t.Fatal("background parent acquired a deadline or cancellation")
	}
	r := &eventsRecorder{}
	var factories atomic.Int32
	o := observerFunc(func(taskstats.Meta) taskstats.Recorder {
		factories.Add(1)
		return r
	})
	taskstats.Begin(Observer(outerCtx, o), taskstats.Meta{}).Run(func() {})
	taskstats.Begin(Observer(context.WithValue(innerCtx, constructionValueKey{}, "derived"), o), taskstats.Meta{}).Run(func() {})
	inner.Activate()
	if factories.Load() != 1 || len(r.snapshot()) != 1 {
		t.Fatal("nested context activated the wrong construction")
	}
	outer.Activate()
	if factories.Load() != 2 || len(r.snapshot()) != 2 {
		t.Fatal("outer construction lost its deferred task")
	}
}

func TestConstructionContextNilParent(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("nil parent did not panic during construction")
		}
	}()
	NewContext(nil) //nolint:staticcheck // Exercise the constructor's explicit nil-parent panic contract.
}

func TestTerminalFirstAndSingleActivation(t *testing.T) {
	ctx, scope := NewContext(context.Background())
	var factories atomic.Int32
	r := &eventsRecorder{}
	o := observerFunc(func(meta taskstats.Meta) taskstats.Recorder {
		factories.Add(1)
		if meta.Executor != "original" {
			t.Errorf("captured metadata = %+v", meta)
		}
		return r
	})
	submitted := time.Now().Add(-time.Second)
	task := taskstats.BeginAt(Observer(ctx, o), taskstats.Meta{Executor: "original"}, submitted)
	task.Run(func() {})
	if factories.Load() != 0 || len(r.snapshot()) != 0 {
		t.Fatal("observation escaped construction")
	}
	scope.Activate()
	scope.Activate()
	events := r.snapshot()
	if factories.Load() != 1 || len(events) != 1 || events[0].State != taskstats.StateFinished || events[0].Outcome != taskstats.OutcomeSuccess || events[0].Submitted != submitted || events[0].Started.IsZero() || events[0].Finished.Before(events[0].Started) {
		t.Fatalf("terminal-first snapshot = %+v; factories=%d", events, factories.Load())
	}
	if scope.records != nil || scope.count != 0 {
		t.Fatal("construction retained recorders after activation")
	}
}

func TestSlowFactoryDoesNotGateTaskOrOtherActivation(t *testing.T) {
	ctx, scope := NewContext(context.Background())
	entered, release, activated := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	r := &eventsRecorder{}
	task := taskstats.Begin(Observer(ctx, observerFunc(func(taskstats.Meta) taskstats.Recorder {
		close(entered)
		<-release
		return r
	})), taskstats.Meta{})
	go func() { scope.Activate(); close(activated) }()
	wait(t, entered)
	ran := make(chan struct{})
	go func() { task.Run(func() {}); close(ran) }()
	wait(t, ran)
	repeated := make(chan struct{})
	go func() { scope.Activate(); close(repeated) }()
	wait(t, repeated)
	releaseOnce.Do(func() { close(release) })
	wait(t, activated)
	if events := r.snapshot(); len(events) != 1 || events[0].State != taskstats.StateFinished {
		t.Fatalf("factory did not observe latest actual exit: %+v", events)
	}
}

func TestDeferredCapacityAndLiveSubmissions(t *testing.T) {
	ctx, scope := NewContext(context.Background())
	var factories, executions atomic.Int32
	o := Observer(ctx, observerFunc(func(taskstats.Meta) taskstats.Recorder {
		factories.Add(1)
		return &eventsRecorder{}
	}))
	for i := 0; i < maxDeferred+1; i++ {
		task := taskstats.Begin(o, taskstats.Meta{})
		if i == maxDeferred && task != nil {
			t.Fatal("overflow admission retained observation")
		}
		task.Run(func() { executions.Add(1) })
	}
	if factories.Load() != 0 || scope.count != maxDeferred || !scope.overflow {
		t.Fatalf("unbounded or early observation: factories=%d scope=%+v", factories.Load(), scope)
	}
	scope.Activate()
	if factories.Load() != maxDeferred || executions.Load() != maxDeferred+1 {
		t.Fatalf("capacity changed execution: factories=%d executions=%d", factories.Load(), executions.Load())
	}
	taskstats.Begin(o, taskstats.Meta{}).Run(func() { executions.Add(1) })
	if factories.Load() != maxDeferred+1 || executions.Load() != maxDeferred+2 {
		t.Fatal("live submission still consumed deferred capacity")
	}
}

func TestLatestRejectsOldAndTerminalRevival(t *testing.T) {
	ctx, scope := NewContext(context.Background())
	r := &eventsRecorder{}
	proxy := Observer(ctx, observerFunc(func(taskstats.Meta) taskstats.Recorder { return r })).NewTask(taskstats.Meta{})
	proxy.Observe(taskstats.Event{Version: 3, State: taskstats.StateFinished, Outcome: taskstats.OutcomeSuccess})
	proxy.Observe(taskstats.Event{Version: 2, State: taskstats.StateRunning})
	proxy.Observe(taskstats.Event{Version: 4, State: taskstats.StateRunning})
	scope.Activate()
	proxy.Observe(taskstats.Event{Version: 5, State: taskstats.StatePending})
	if events := r.snapshot(); len(events) != 1 || events[0].Version != 3 || events[0].State != taskstats.StateFinished {
		t.Fatalf("terminal state revived: %+v", events)
	}
}

func TestActivationDeliveryMayArriveOutOfOrder(t *testing.T) {
	ctx, scope := NewContext(context.Background())
	entered, release, activated := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	r := &eventsRecorder{}
	proxy := Observer(ctx, observerFunc(func(taskstats.Meta) taskstats.Recorder {
		return recorderFunc(func(event taskstats.Event) {
			if event.Version == 2 {
				close(entered)
				<-release
			}
			r.Observe(event)
		})
	})).NewTask(taskstats.Meta{})
	proxy.Observe(taskstats.Event{Version: 2, State: taskstats.StateRunning})
	go func() { scope.Activate(); close(activated) }()
	wait(t, entered)
	proxy.Observe(taskstats.Event{Version: 3, State: taskstats.StateFinished})
	releaseOnce.Do(func() { close(release) })
	wait(t, activated)
	if events := r.snapshot(); len(events) != 2 || events[0].Version != 3 || events[1].Version != 2 {
		t.Fatalf("unexpected delivery contract: %+v", events)
	}
}

func TestFactoryFailureDoesNotPreventOtherActivation(t *testing.T) {
	ctx, scope := NewContext(context.Background())
	r := &eventsRecorder{}
	for _, o := range []observerFunc{
		func(taskstats.Meta) taskstats.Recorder { panic("factory") },
		func(taskstats.Meta) taskstats.Recorder { return nil },
		func(taskstats.Meta) taskstats.Recorder { return r },
	} {
		taskstats.Begin(Observer(ctx, o), taskstats.Meta{}).Run(func() {})
	}
	scope.Activate()
	if events := r.snapshot(); len(events) != 1 || events[0].State != taskstats.StateFinished {
		t.Fatalf("one extension prevented another: %+v", events)
	}
}
