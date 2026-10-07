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

package client

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	internalclient "github.com/cloudwego/kitex/internal/client"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/taskstats"
	"github.com/cloudwego/kitex/pkg/utils"
)

type timeoutObserver struct {
	mu     sync.Mutex
	events []taskstats.Event
	meta   taskstats.Meta
	final  chan struct{}
}

func (o *timeoutObserver) NewTask(m taskstats.Meta) taskstats.Recorder {
	o.mu.Lock()
	o.meta = m
	o.mu.Unlock()
	return o
}

func (o *timeoutObserver) Observe(e taskstats.Event) {
	o.mu.Lock()
	o.events = append(o.events, e)
	o.mu.Unlock()
	if e.State == taskstats.StateFinished {
		close(o.final)
	}
}

func TestObservedTimeoutExecutionOutlivesCaller(t *testing.T) {
	for _, tc := range []struct {
		name     string
		maxIdle  int
		executor string
	}{{"reuse", 2, "kitex.timeout.reuse"}, {"overflow", 1, "kitex.timeout.overflow"}} {
		t.Run(tc.name, func(t *testing.T) {
			p := newTimeoutPool(tc.maxIdle, 20*time.Millisecond)
			o := &timeoutObserver{final: make(chan struct{})}
			started, release := make(chan struct{}), make(chan struct{})
			returned := make(chan error, 1)
			go func() {
				_, err := p.runTask(context.Background(), 20*time.Millisecond, nil, nil,
					func(context.Context, any, any) error { close(started); <-release; return nil }, o, "configured")
				returned <- err
			}()
			<-started
			if err := <-returned; err != context.DeadlineExceeded {
				t.Fatalf("caller %v", err)
			}
			o.mu.Lock()
			last := o.events[len(o.events)-1]
			if last.State != taskstats.StateRunning || !last.CallerTimedOut || o.meta.Executor != tc.executor || o.meta.Service != "configured" {
				t.Errorf("last/meta %+v %+v", last, o.meta)
			}
			o.mu.Unlock()
			select {
			case <-o.final:
				t.Fatal("timeout ended live execution")
			default:
			}
			snapshot := p.snapshot()
			want := int64(1)
			if tc.maxIdle == 1 {
				want = 0
			}
			if snapshot.Workers != want || snapshot.MaxIdle != int64(tc.maxIdle) || snapshot.QueueCapacity != 0 || !snapshot.QueueCapacityKnown {
				t.Fatalf("snapshot %+v", snapshot)
			}
			close(release)
			<-o.final
			o.mu.Lock()
			final := o.events[len(o.events)-1]
			o.mu.Unlock()
			if final.Outcome != taskstats.OutcomeSuccess || !final.CallerTimedOut || final.Finished.Before(final.Started) {
				t.Fatalf("final %+v", final)
			}
			deadline := time.Now().Add(time.Second)
			for p.Size() != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if p.Size() != 0 {
				t.Fatal("retained worker did not expire")
			}
		})
	}
}

func TestObservedTimeoutPanicAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		panic bool
	}{{"panic", true}, {"cancel", false}} {
		t.Run(tc.name, func(t *testing.T) {
			p := newTimeoutPool(1, time.Millisecond)
			o := &timeoutObserver{final: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			returned := make(chan error, 1)
			go func() {
				_, err := p.runTask(ctx, 0, nil, nil, func(context.Context, any, any) error {
					close(entered)
					<-release
					if tc.panic {
						panic("biz")
					}
					return errors.New("actual")
				}, o, "")
				returned <- err
			}()
			<-entered
			if !tc.panic {
				cancel()
				if err := <-returned; err != context.Canceled {
					t.Fatal(err)
				}
			}
			close(release)
			if tc.panic {
				if err := <-returned; err == nil || !strings.Contains(err.Error(), "biz") {
					t.Fatal(err)
				}
			}
			<-o.final
			o.mu.Lock()
			final := o.events[len(o.events)-1]
			o.mu.Unlock()
			if tc.panic && final.Outcome != taskstats.OutcomePanic {
				t.Fatalf("%+v", final)
			}
			if !tc.panic && (final.Outcome != taskstats.OutcomeError || !final.CallerCanceled) {
				t.Fatalf("%+v", final)
			}
		})
	}
}

func TestWithTaskObserverAndTimeoutMiddleware(t *testing.T) {
	o := &timeoutObserver{final: make(chan struct{})}
	opts := &internalclient.Options{}
	var debug utils.Slice
	WithTaskObserver(o).F(opts, &debug)
	if opts.TaskObserver != o || len(debug) != 1 {
		t.Fatal("observer not wired")
	}
	WithTaskObserver(nil).F(opts, &debug)
	if opts.TaskObserver != nil {
		t.Fatal("nil did not disable")
	}
	cfg := rpcinfo.NewRPCConfig()
	rpcinfo.AsMutableRPCConfig(cfg).SetRPCTimeout(time.Second)
	ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), rpcinfo.NewRPCInfo(nil, rpcinfo.NewEndpointInfo("svc", "method", nil, nil), nil, cfg, rpcinfo.NewRPCStats()))
	// This is the same middleware constructor used by kClient.initMiddlewares.
	ep := rpcTimeoutMWWithObserver(context.Background(), o, "svc")(func(context.Context, any, any) error { return nil })
	if err := ep(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	<-o.final
	if o.meta.Service != "svc" {
		t.Fatal("configured service missing")
	}
}

func TestEndpointContextErrorsAreExecutionErrors(t *testing.T) {
	for _, epErr := range []error{context.Canceled, context.DeadlineExceeded} {
		p := newTimeoutPool(1, time.Millisecond)
		o := &timeoutObserver{final: make(chan struct{})}
		_, err := p.runTask(context.Background(), time.Second, nil, nil, func(context.Context, any, any) error { return epErr }, o, "")
		if err != epErr {
			t.Fatal(err)
		}
		<-o.final
		o.mu.Lock()
		final := o.events[len(o.events)-1]
		o.mu.Unlock()
		if final.Outcome != taskstats.OutcomeError || final.CallerTimedOut || final.CallerCanceled {
			t.Fatalf("endpoint error mislabeled caller cause %+v", final)
		}
	}
}

func TestTimeoutTaskGoexitIsAborted(t *testing.T) {
	p := newTimeoutPool(1, time.Millisecond)
	o := &timeoutObserver{final: make(chan struct{})}
	_, err := p.runTask(context.Background(), time.Second, nil, nil, func(context.Context, any, any) error { runtime.Goexit(); return nil }, o, "")
	// Keep the existing timeout task behavior: Goexit wakes the caller without
	// manufacturing an RPC error; only its observation outcome is corrected.
	if err != nil {
		t.Fatal(err)
	}
	<-o.final
	o.mu.Lock()
	final := o.events[len(o.events)-1]
	o.mu.Unlock()
	if final.Outcome != taskstats.OutcomeAborted {
		t.Fatalf("%+v", final)
	}
}

type panickingTaskObserver struct{}

func (panickingTaskObserver) NewTask(taskstats.Meta) taskstats.Recorder { panic("broken") }

func TestTimeoutObserverFailureDoesNotStrandWorker(t *testing.T) {
	p := newTimeoutPool(2, time.Millisecond)
	_, err := p.runTask(context.Background(), time.Second, nil, nil, func(context.Context, any, any) error { return nil }, panickingTaskObserver{}, "")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for p.Size() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if p.Size() != 0 {
		t.Fatal("failed observer stranded worker")
	}
}

func BenchmarkTimeoutPoolObserverDisabled(b *testing.B) {
	p := newTimeoutPool(1, time.Millisecond)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = p.runTask(context.Background(), 0, nil, nil, noop, nil, "")
	}
}

func TestTimeoutTaskPanicNilOutcome(t *testing.T) {
	p := newTimeoutPool(1, time.Millisecond)
	o := &timeoutObserver{final: make(chan struct{})}
	_, _ = p.runTask(context.Background(), time.Second, nil, nil, func(context.Context, any, any) error { panic(nil) }, o, "")
	<-o.final
	o.mu.Lock()
	final := o.events[len(o.events)-1]
	o.mu.Unlock()
	if final.Outcome != taskstats.OutcomePanic {
		t.Fatalf("%+v", final)
	}
}
