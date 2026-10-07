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

	"github.com/cloudwego/kitex/pkg/taskstats"
)

type testObserver struct {
	mu       sync.Mutex
	events   []taskstats.Event
	metas    []taskstats.Meta
	finished chan struct{}
}

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
	snapshot := DefaultPoolSnapshot()
	if !snapshot.WorkersKnown || snapshot.QueueCapacityKnown || snapshot.MaxIdleKnown {
		t.Fatal("unsupported global capabilities reported")
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
