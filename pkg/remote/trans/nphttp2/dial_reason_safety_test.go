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

package nphttp2

import (
	"errors"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/gofunc"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/connstats"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
)

type dialReasonError struct{ hook func() }

func (*dialReasonError) Error() string { return "custom dial failure" }

type dialReasonIsError struct{ *dialReasonError }

func (e dialReasonIsError) Is(error) bool { e.hook(); return false }

type dialReasonAsError struct{ *dialReasonError }

func (e dialReasonAsError) As(interface{}) bool { e.hook(); return false }

type dialReasonUnwrapError struct{ *dialReasonError }

func (e dialReasonUnwrapError) Unwrap() error { e.hook(); return errors.New("underlying failure") }

// Deliberately has no Is/As/Unwrap methods: ordinary net.Error.Timeout is also
// application code, rather than an observer callback or an error-chain hook.
type dialReasonNetError struct{ *dialReasonError }

func (e dialReasonNetError) Timeout() bool { e.hook(); return false }
func (dialReasonNetError) Temporary() bool { return false }

func TestDialReasonErrorMethodsOutsideConstruction(t *testing.T) {
	for _, method := range []string{"is", "as", "unwrap", "net-error"} {
		for _, exit := range []string{"return", "panic", "panic-nil", "goexit"} {
			t.Run(method+"/"+exit, func(t *testing.T) {
				observer := &poolDialObserver{}
				pool := observerPool(t, observer, false)
				transports := newTransports(1)
				pool.conns.Store(mockAddr0, transports)
				slot := transports.getTransportSlot(0)
				ctx := newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional)
				var entered atomic.Bool
				base := &dialReasonError{hook: func() {
					if !entered.CompareAndSwap(false, true) {
						return
					}
					// Detect the old lock placement without hanging the failing test.
					if !slot.mu.TryLock() {
						t.Error("dial error method called with slot mutex held")
						return
					}
					slot.mu.Unlock()
					// A real same-address Get must create a fresh flight, rather than
					// join the flight whose error is currently being classified.
					nestedFailure := errors.New("nested dial failure")
					conn, err := pool.Get(ctx, "tcp", mockAddr0, remote.ConnOption{Dialer: newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) {
						return nil, nestedFailure
					})})
					if conn != nil || err != nestedFailure {
						t.Errorf("reentrant Get = (%v, %v)", conn, err)
					}
					switch exit {
					case "panic":
						panic("error method panic")
					case "panic-nil":
						panic(nil)
					case "goexit":
						runtime.Goexit()
					}
				}}
				var failure error
				switch method {
				case "is":
					failure = dialReasonIsError{base}
				case "as":
					failure = dialReasonAsError{base}
				case "unwrap":
					failure = dialReasonUnwrapError{base}
				default:
					failure = dialReasonNetError{base}
				}
				finished := make(chan struct{})
				returned := false
				go func() {
					defer close(finished)
					conn, err := pool.Get(ctx, "tcp", mockAddr0, remote.ConnOption{Dialer: newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) {
						return nil, failure
					})})
					returned = true
					if conn != nil || err != failure {
						t.Errorf("classification changed original dial failure: (%v, %v)", conn, err)
					}
				}()
				select {
				case <-finished:
				case <-time.After(3 * time.Second):
					t.Fatal("dial error classification blocked reentrant Get")
				}
				if !entered.Load() || returned != (exit != "goexit") {
					t.Errorf("error hook entered=%v; Get returned=%v", entered.Load(), returned)
				}
				events, _ := observer.snapshot()
				if len(events) != 2 {
					t.Fatalf("completed original and nested attempts = %d, want 2", len(events))
				}
				want := connstats.ReasonOther
				if method == "net-error" && exit == "return" {
					want = connstats.ReasonNetwork
				}
				if event := events[1]; event.Outcome != connstats.DialFailed || event.Reason != want {
					t.Errorf("original dial event = %+v, want failed/%v", event, want)
				}
			})
		}
	}
}

type dialReasonFailWriteConn struct {
	net.Conn
	failure error
}

func (c *dialReasonFailWriteConn) Write([]byte) (int, error) { return 0, c.failure }

type dialReasonMetricsObserver struct{ *poolDialObserver }

func (dialReasonMetricsObserver) ConnectionMetricsEnabled() {}

func TestDialReasonGoexitStillActivatesConstructionTasks(t *testing.T) {
	defer gofunc.SetTaskObserver(nil)
	observer := &poolDialObserver{}
	pool := observerPool(t, dialReasonMetricsObserver{observer}, false)
	transports := newTransports(1)
	pool.conns.Store(mockAddr0, transports)
	slot := transports.getTransportSlot(0)
	var owned *constructionCloseConn
	var classified atomic.Bool
	failure := dialReasonIsError{&dialReasonError{hook: func() {
		if !slot.mu.TryLock() {
			t.Error("transport failure classification held slot mutex")
			return
		}
		slot.mu.Unlock()
		select {
		case <-owned.closed:
		default:
			t.Error("transport error classified before socket cleanup")
		}
		classified.Store(true)
		runtime.Goexit()
	}}}
	tasks := &constructionTaskObserver{changed: make(chan struct{}, 8), factory: func() {
		if !classified.Load() {
			t.Error("task factory ran before dial classification")
		}
	}}
	gofunc.SetTaskObserver(tasks)
	dialer := newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) {
		conn, _ := newConnectionPipe(t)
		owned = &constructionCloseConn{Conn: conn, closed: make(chan struct{})}
		return &dialReasonFailWriteConn{Conn: owned, failure: failure}, nil
	})
	finished := make(chan struct{})
	returned := false
	go func() {
		defer close(finished)
		_, _ = pool.Get(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), "tcp", mockAddr0, remote.ConnOption{Dialer: dialer})
		returned = true
	}()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("failed construction did not unwind")
	}
	if returned || !classified.Load() {
		t.Errorf("classification Goexit was changed: returned=%v, classified=%v", returned, classified.Load())
	}
	tasks.waitFinished(t, 1) // the accepted real fallback reader must still be activated
	events, _ := observer.snapshot()
	if len(events) != 1 || events[0].Outcome != connstats.DialTransportFailed || events[0].Reason != connstats.ReasonOther {
		t.Fatalf("failed transport dial events = %+v", events)
	}
	if !slot.mu.TryLock() {
		t.Fatal("slot remains locked after classification Goexit")
	}
	slot.mu.Unlock()
	_, _, _ = pool.sfg.Do(mockAddr0, func() (interface{}, error) { return nil, nil })
}
