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

package gofunc

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/gopkg/util/gopool"

	"github.com/cloudwego/kitex/pkg/taskstats"
)

type testObserver struct {
	mu       sync.Mutex
	events   []taskstats.Event
	metas    []taskstats.Meta
	finished chan struct{}
}

type observationTestKey string

func (o *testObserver) NewTask(m taskstats.Meta) taskstats.Recorder {
	o.mu.Lock()
	o.metas = append(o.metas, m)
	o.mu.Unlock()
	return o
}

func (o *testObserver) Observe(e taskstats.Event) {
	o.mu.Lock()
	o.events = append(o.events, e)
	o.mu.Unlock()
	if e.State == taskstats.StateFinished && o.finished != nil {
		close(o.finished)
	}
}

func TestWrapSynchronousExecutorAndPanic(t *testing.T) {
	for _, panics := range []bool{false, true} {
		o := &testObserver{}
		wrapped := WrapTask(func(_ context.Context, f func()) { f() }, o, taskstats.Meta{Executor: "custom", Type: "inline"})
		func() {
			defer func() {
				p := recover()
				if panics && p != "biz" {
					t.Errorf("panic %v", p)
				}
			}()
			wrapped(context.Background(), func() {
				if panics {
					panic("biz")
				}
			})
		}()
		if len(o.events) != 3 || o.events[2].State != taskstats.StateFinished {
			t.Fatalf("events %+v", o.events)
		}
		want := taskstats.OutcomeSuccess
		if panics {
			want = taskstats.OutcomePanic
		}
		if o.events[2].Outcome != want {
			t.Fatal("wrong outcome")
		}
	}
	o := &testObserver{}
	wrapped := WrapTask(func(context.Context, func()) { panic("submit") }, o, taskstats.Meta{})
	func() {
		defer func() {
			if recover() != "submit" {
				t.Error("submit panic altered")
			}
		}()
		wrapped(context.Background(), func() { t.Error("rejected task ran") })
	}()
	if o.events[len(o.events)-1].Outcome != taskstats.OutcomeError {
		t.Fatal("submission failure leaked pending")
	}
}

func TestDefaultObserverSwitchKeepsTaskOwner(t *testing.T) {
	defer SetTaskObserver(nil)
	first := &testObserver{finished: make(chan struct{})}
	second := &testObserver{finished: make(chan struct{})}
	started, release := make(chan struct{}), make(chan struct{})
	SetTaskObserver(first)
	GoFunc(context.Background(), func() { close(started); <-release })
	<-started
	SetTaskObserver(second)
	close(release)
	<-first.finished
	GoFunc(context.Background(), func() {})
	<-second.finished
	for _, o := range []*testObserver{first, second} {
		o.mu.Lock()
		if len(o.events) != 3 || len(o.metas) != 1 || o.metas[0].Service != "" {
			t.Errorf("events/metas %+v %+v", o.events, o.metas)
		}
		o.mu.Unlock()
	}
}

func TestDefaultPoolSnapshotSharedWorkersOnly(t *testing.T) {
	defer SetTaskObserver(nil)
	o := &testObserver{}
	SetTaskObserver(o)
	started, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		close(release)
		select {
		case <-exited:
		case <-time.After(2 * time.Second):
			t.Error("shared gopool task did not exit")
		}
	})
	// Direct users of the shared pool contribute workers without producing
	// Kitex task events. Hold one worker so unrelated pool activity cannot
	// invalidate the assertion; do not require a global count delta or zero.
	gopool.CtxGo(context.Background(), func() {
		defer close(exited)
		close(started)
		<-release
	})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("shared gopool task did not start")
	}
	snapshot := DefaultPoolSnapshot()
	if !snapshot.WorkersKnown || snapshot.Workers < 1 {
		t.Fatalf("shared worker missing: %+v", snapshot)
	}
	want := taskstats.PoolSnapshot{Workers: snapshot.Workers, WorkersKnown: true}
	if snapshot != want {
		t.Fatalf("unsupported physical capabilities reported: %+v", snapshot)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.events) != 0 || len(o.metas) != 0 {
		t.Fatalf("direct gopool task entered Kitex observation: %+v %+v", o.events, o.metas)
	}
}

func TestWrapPendingDelay(t *testing.T) {
	o := &testObserver{}
	var accepted func()
	wrapped := WrapTask(func(_ context.Context, f func()) { accepted = f }, o, taskstats.Meta{Executor: "delayed"})
	wrapped(context.Background(), func() {})
	if len(o.events) != 1 || o.events[0].State != taskstats.StatePending {
		t.Fatal("pending not observed")
	}
	accepted()
	if len(o.events) != 3 || o.events[1].Started.Before(o.events[0].Submitted) {
		t.Fatal("delay times invalid")
	}
}

func TestRecoverHelperObservesPanicOnce(t *testing.T) {
	defer SetTaskObserver(nil)
	SetPanicHandler(nil)
	for _, tc := range []struct {
		name string
		f    func()
		want taskstats.Outcome
	}{{"panic", func() { panic("biz") }, taskstats.OutcomePanic}, {"panic_nil", func() { panic(nil) }, taskstats.OutcomePanic}, {"goexit", runtime.Goexit, taskstats.OutcomeAborted}} {
		t.Run(tc.name, func(t *testing.T) {
			o := &testObserver{finished: make(chan struct{})}
			SetTaskObserver(o)
			// A synchronous custom dispatcher proves that the recover helper
			// is independent of an assumption about the global physical pool.
			original := GoFunc
			GoFunc = func(_ context.Context, f func()) { go f() }
			defer func() { GoFunc = original }()
			RecoverGoFuncWithInfo(context.Background(), tc.f, NewBasicInfo("service", "remote"))
			<-o.finished
			o.mu.Lock()
			defer o.mu.Unlock()
			if len(o.metas) != 1 || len(o.events) != 3 || o.events[2].Outcome != tc.want {
				t.Fatalf("%+v %+v", o.metas, o.events)
			}
		})
	}
	o := &testObserver{finished: make(chan struct{})}
	SetTaskObserver(o)
	RecoverGoFuncWithInfo(context.Background(), func() { panic("biz") }, NewBasicInfo("service", "remote"))
	<-o.finished
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.metas) != 1 || len(o.events) != 3 || o.events[2].Outcome != taskstats.OutcomePanic {
		t.Fatal("default/helper duplicated or swallowed panic observation")
	}
}

func TestRecoverHelperWithWrappedDispatcherHasOneOwner(t *testing.T) {
	defer SetTaskObserver(nil)
	SetPanicHandler(nil)
	o := &testObserver{finished: make(chan struct{})}
	SetTaskObserver(o)
	original := GoFunc
	GoFunc = WrapTask(func(_ context.Context, f func()) { f() }, o, taskstats.Meta{Executor: "custom"})
	defer func() { GoFunc = original }()
	RecoverGoFuncWithInfo(context.Background(), func() { panic("biz") }, NewBasicInfo("service", "remote"))
	<-o.finished
	if len(o.metas) != 1 || len(o.events) != 3 || o.events[2].Outcome != taskstats.OutcomePanic {
		t.Fatal("wrapped recover dispatcher counted twice")
	}
}

func gofuncBusinessPanic() { panic("original") }

func TestRecoverRetainsOriginalBusinessStack(t *testing.T) {
	defer SetTaskObserver(nil)
	defer SetPanicHandler(nil)
	for _, enabled := range []bool{false, true} {
		if enabled {
			SetTaskObserver(&testObserver{})
		} else {
			SetTaskObserver(nil)
		}
		stacks := make(chan string, 1)
		SetPanicHandler(func(_ *Info, _ interface{}, stack string) { stacks <- stack })
		RecoverGoFuncWithInfo(context.Background(), gofuncBusinessPanic, NewBasicInfo("svc", "addr"))
		if stack := <-stacks; !strings.Contains(stack, "gofuncBusinessPanic") {
			t.Fatalf("business stack lost enabled=%v: %s", enabled, stack)
		}
	}
}

func TestRecoverDerivedHandoffAndIndependentChildren(t *testing.T) {
	for _, derivation := range []string{"value", "cancel", "deadline"} {
		t.Run(derivation, func(t *testing.T) {
			defer SetTaskObserver(nil)
			o := &testObserver{}
			SetTaskObserver(o)
			original := GoFunc
			defer func() { GoFunc = original }()
			var queued func()
			var handoff context.Context
			var cancel context.CancelFunc = func() {}
			wrapped := WrapTask(func(_ context.Context, f func()) { queued = f }, o, taskstats.Meta{Executor: "derived"})
			GoFunc = func(ctx context.Context, f func()) {
				switch derivation {
				case "value":
					ctx = context.WithValue(ctx, observationTestKey("test-value"), "preserved")
				case "cancel":
					ctx, cancel = context.WithCancel(ctx)
				case "deadline":
					ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				}
				handoff = ctx
				wrapped(ctx, f)
			}
			defer cancel()
			inline := WrapTask(func(_ context.Context, f func()) { f() }, o, taskstats.Meta{Executor: "child"})
			RecoverGoFuncWithInfo(context.Background(), func() {
				if observationOwned(handoff) {
					t.Error("handoff still owns independent child after entry")
				}
				inline(context.WithValue(handoff, observationTestKey("child-value"), true), func() {})
			}, NewBasicInfo("service", "remote"))
			cancel() // cancellation must not retire a still-accepted handoff
			if !observationOwned(handoff) || len(o.metas) != 1 {
				t.Fatalf("derived handoff duplicated/retired before entry: %+v", o.metas)
			}
			queued()
			if observationOwned(handoff) || len(o.metas) != 2 {
				t.Fatalf("independent child suppressed: %+v", o.metas)
			}
			if derivation == "value" && handoff.Value(observationTestKey("test-value")) != "preserved" {
				t.Fatal("ordinary context value lost")
			}
			finished := 0
			for _, event := range o.events {
				if event.State == taskstats.StateFinished {
					finished++
				}
			}
			if finished != 2 {
				t.Fatalf("terminal count = %d, want helper and child", finished)
			}
		})
	}
}

func TestRecoverDerivedHandoffThroughDefault(t *testing.T) {
	defer SetTaskObserver(nil)
	o := &testObserver{finished: make(chan struct{})}
	SetTaskObserver(o)
	original := GoFunc
	defer func() { GoFunc = original }()
	GoFunc = func(ctx context.Context, f func()) {
		ctx, cancel := context.WithCancel(context.WithValue(ctx, observationTestKey("test-value"), true))
		defer cancel()
		original(ctx, f)
	}
	RecoverGoFuncWithInfo(context.Background(), func() {}, NewBasicInfo("service", "remote"))
	select {
	case <-o.finished:
	case <-time.After(2 * time.Second):
		t.Fatal("derived default handoff did not execute")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.metas) != 1 || len(o.events) != 3 {
		t.Fatalf("derived default handoff duplicated: %+v %+v", o.metas, o.events)
	}
}

func TestRecoverIndependentRootsAndSubmissionFailure(t *testing.T) {
	defer SetTaskObserver(nil)
	o := &testObserver{}
	SetTaskObserver(o)
	original := GoFunc
	defer func() { GoFunc = original }()
	var queued []func()
	var contexts []context.Context
	GoFunc = func(ctx context.Context, f func()) {
		contexts = append(contexts, context.WithValue(ctx, observationTestKey("test-value"), true))
		queued = append(queued, f)
	}
	base := context.Background()
	for i := 0; i < 2; i++ {
		RecoverGoFuncWithInfo(base, func() {}, NewBasicInfo("service", "remote"))
	}
	queued[0]()
	if observationOwned(contexts[0]) || !observationOwned(contexts[1]) {
		t.Fatal("roots reused one ownership token")
	}
	queued[1]()
	GoFunc = func(ctx context.Context, _ func()) {
		contexts = append(contexts, ctx)
		panic("submit")
	}
	func() {
		defer func() {
			if recover() != "submit" {
				t.Error("submission panic changed")
			}
		}()
		RecoverGoFuncWithInfo(base, func() { t.Error("rejected function ran") }, NewBasicInfo("service", "remote"))
	}()
	if observationOwned(contexts[2]) || len(o.metas) != 3 || o.events[len(o.events)-1].Outcome != taskstats.OutcomeError {
		t.Fatalf("submission failure did not retire owner: %+v", o.events)
	}
}

func TestRecoverConcurrentRootsReuseParentContext(t *testing.T) {
	defer SetTaskObserver(nil)
	o := &testObserver{}
	SetTaskObserver(o)
	original := GoFunc
	defer func() { GoFunc = original }()
	const roots = 32
	var submitted, finished sync.WaitGroup
	submitted.Add(roots)
	finished.Add(roots)
	GoFunc = func(ctx context.Context, f func()) {
		original(context.WithValue(ctx, observationTestKey("derived"), true), func() {
			defer finished.Done()
			f()
		})
	}
	parent := context.WithValue(context.Background(), observationTestKey("shared-parent"), true)
	for i := 0; i < roots; i++ {
		go func() {
			defer submitted.Done()
			RecoverGoFuncWithInfo(parent, func() {}, NewBasicInfo("service", "remote"))
		}()
	}
	done := make(chan struct{})
	go func() { submitted.Wait(); finished.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent root submissions did not join")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.metas) != roots || len(o.events) != 3*roots {
		t.Fatalf("concurrent roots lost/duplicated ownership: tasks=%d events=%d", len(o.metas), len(o.events))
	}
}
