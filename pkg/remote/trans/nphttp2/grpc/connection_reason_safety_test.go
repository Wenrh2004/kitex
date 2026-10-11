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

package grpc

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/remote/connstats"
)

type connectionReasonSafetyError struct{ call func() }

func (*connectionReasonSafetyError) Error() string { return "close reason safety fixture" }

type connectionReasonSafetyIsError struct{ *connectionReasonSafetyError }

func (e connectionReasonSafetyIsError) Is(target error) bool {
	if target == errGracefulShutdown {
		return false
	}
	e.call()
	return false
}

type connectionReasonSafetyAsError struct{ *connectionReasonSafetyError }

func (e connectionReasonSafetyAsError) As(interface{}) bool {
	e.call()
	return false
}

type connectionReasonSafetyUnwrapError struct{ *connectionReasonSafetyError }

// loopy.run checks this private sentinel before transport cleanup. Keep that
// existing writer path separate from the close-reason classification probe.
func (connectionReasonSafetyUnwrapError) Is(target error) bool {
	return target == errGracefulShutdown
}

func (e connectionReasonSafetyUnwrapError) Unwrap() error {
	e.call()
	return errors.New("unclassified fixture cause")
}

type connectionReasonSafetyTimeoutError struct{ *connectionReasonSafetyError }

func (e connectionReasonSafetyTimeoutError) Timeout() bool {
	e.call()
	return false
}

func (connectionReasonSafetyTimeoutError) Temporary() bool { return false }

// Probe before actually reentering, so the unfixed implementation fails without
// leaving a deadlocked Close goroutine or preventing the fixture's cleanup.
func connectionReasonSafetyReenter(t *testing.T, client *http2Client) bool {
	t.Helper()
	select {
	case <-client.conn.(*connectionObserverTestConn).closed:
	default:
		if client.mu.TryLock() {
			client.mu.Unlock()
		} else {
			t.Error("close reason error method ran while transport mutex was held")
		}
		t.Error("close reason error method ran before socket cleanup")
		return false
	}
	// Other shutdown goroutines may briefly own the lock. A classifier must
	// not wait for reader/writer exit: it can itself be running on one of them.
	deadline := time.Now().Add(100 * time.Millisecond)
	locked := client.mu.TryLock()
	for !locked && time.Now().Before(deadline) {
		runtime.Gosched()
		locked = client.mu.TryLock()
	}
	if !locked {
		t.Error("close reason error method ran while transport mutex was held")
		return false
	}
	client.mu.Unlock()
	if client.IsActive() {
		t.Error("close reason error method ran before transport was marked closing")
	}
	return true
}

func connectionReasonSafetyAwaitCleanup(t *testing.T, client *http2Client, stream *Stream) {
	t.Helper()
	awaitConnectionSignal(t, client.ctx.Done())
	awaitConnectionSignal(t, client.conn.(*connectionObserverTestConn).closed)
	if stream != nil {
		awaitConnectionSignal(t, stream.done)
		awaitConnectionSignal(t, stream.headerChan)
	}
	awaitConnectionSignal(t, client.readerDone)
	awaitConnectionSignal(t, client.writerDone)
}

func TestConnectionCloseReasonErrorMethodsOutsideLockAndPanicCleanup(t *testing.T) {
	methods := []struct {
		name      string
		makeError func(func()) error
		reason    connstats.Reason
	}{
		{"Is", func(call func()) error {
			return connectionReasonSafetyIsError{&connectionReasonSafetyError{call: call}}
		}, connstats.ReasonOther},
		{"As", func(call func()) error {
			return connectionReasonSafetyAsError{&connectionReasonSafetyError{call: call}}
		}, connstats.ReasonOther},
		{"Unwrap", func(call func()) error {
			return connectionReasonSafetyUnwrapError{&connectionReasonSafetyError{call: call}}
		}, connstats.ReasonOther},
		{"Timeout", func(call func()) error {
			return connectionReasonSafetyTimeoutError{&connectionReasonSafetyError{call: call}}
		}, connstats.ReasonNetwork},
	}
	for _, method := range methods {
		for _, behavior := range []string{"reenter", "panic", "panic-nil", "goexit"} {
			t.Run(method.name+"/"+behavior, func(t *testing.T) {
				r := new(connectionMetricsRecorder)
				client := newMetricsTransport(t, r)
				stream, err := client.NewStream(context.Background(), &CallHdr{Method: "/service/method"})
				if err != nil {
					t.Fatal(err)
				}
				var calls atomic.Int32
				closeReasonErr := method.makeError(func() {
					calls.Add(1)
					if !connectionReasonSafetyReenter(t, client) {
						return
					}
					switch behavior {
					case "panic":
						panic("close reason classifier panic")
					case "panic-nil":
						panic(nil)
					case "goexit":
						runtime.Goexit()
					}
				})
				finished := make(chan struct{})
				var returned, escapedPanic atomic.Bool
				go func() {
					defer close(finished)
					closeReturned := false
					func() {
						defer func() { _ = recover() }()
						if err := client.Close(closeReasonErr); err != nil {
							t.Errorf("Close returned socket error: %v", err)
						}
						closeReturned = true
					}()
					// A recovered legacy panic(nil) returns here; Goexit only
					// runs the deferred signal and never reaches this branch.
					if !closeReturned {
						escapedPanic.Store(true)
						return
					}
					returned.Store(true)
				}()
				awaitConnectionSignal(t, finished)
				if escapedPanic.Load() {
					t.Error("close reason error panic escaped Close")
				}
				if behavior == "goexit" {
					if returned.Load() {
						t.Error("Close swallowed the classifier's Goexit")
					}
				} else if !returned.Load() {
					t.Error("Close did not return after reason classification")
				}
				if calls.Load() == 0 {
					t.Error("close reason classifier did not exercise error method")
				}
				connectionReasonSafetyAwaitCleanup(t, client, stream)
				latest, events := r.snapshot()
				wantReason := method.reason
				if behavior != "reenter" {
					wantReason = connstats.ReasonOther
				}
				if latest.State != connstats.StateClosed || latest.CloseReason != wantReason {
					t.Errorf("terminal observation = %+v, want Closed with reason %v", latest, wantReason)
				}
				if !latest.Streams.Terminal || latest.Streams.Active != 0 || latest.Streams.Waiting != 0 || latest.Streams.Capacity != 0 {
					t.Errorf("terminal observation retained live stream gauges: %+v", latest.Streams)
				}
				for _, event := range events {
					if event.State == connstats.StateClosed && event.CloseReason == connstats.ReasonNone {
						t.Error("published terminal observation before close reason was determined")
					}
				}
			})
		}
	}
}

func TestConnectionCloseReasonBlockedClassificationDefersTerminalPublication(t *testing.T) {
	r := new(connectionMetricsRecorder)
	conn := newConnectionObserverTestConn()
	tr, err := NewClientTransportWithConfig(context.Background(), conn, ConnectOptions{
		ConnectionObserver: connectionMetricsObserver{factory: func(connstats.Meta) connstats.Recorder { return r }},
	}, ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	client := tr.(*http2Client)
	client.ActivateConnectionObserver()
	// An installed recorder must not receive a Closed event before the final
	// reason is known, even when another publisher or activation races Close.
	t.Cleanup(func() {
		_ = client.Close(ErrConnClosing)
		connectionReasonSafetyAwaitCleanup(t, client, nil)
	})
	stream, err := client.NewStream(context.Background(), &CallHdr{Method: "/service/method"})
	if err != nil {
		t.Fatal(err)
	}
	entered, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	var blocked atomic.Bool
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	closeReasonErr := connectionReasonSafetyTimeoutError{&connectionReasonSafetyError{call: func() {
		if !connectionReasonSafetyReenter(t, client) {
			enteredOnce.Do(func() { close(entered) })
			return
		}
		blocked.Store(true)
		enteredOnce.Do(func() { close(entered) })
		<-release
	}}}
	t.Cleanup(func() {
		unblock()
		awaitConnectionSignal(t, closed)
	})
	go func() {
		defer close(closed)
		if err := client.Close(closeReasonErr); err != nil {
			t.Errorf("Close returned socket error: %v", err)
		}
	}()
	awaitConnectionSignal(t, entered)
	if !blocked.Load() {
		// The lock probe already reported the unfixed behavior; do not try to
		// activate while its classifier holds the transport mutex.
		awaitConnectionSignal(t, closed)
		return
	}
	connectionReasonSafetyAwaitCleanup(t, client, stream)
	activated := make(chan struct{})
	go func() {
		defer close(activated)
		client.ActivateConnectionObserver()
		client.publishConnectionObservation()
	}()
	t.Cleanup(func() { unblock(); awaitConnectionSignal(t, activated) })
	awaitConnectionSignal(t, activated)
	_, events := r.snapshot()
	for _, event := range events {
		if event.State == connstats.StateClosed {
			t.Errorf("activation published terminal state during pending classification: %+v", event)
		}
	}
	unblock()
	awaitConnectionSignal(t, closed)
	latest, events := r.snapshot()
	if latest.State != connstats.StateClosed || latest.CloseReason != connstats.ReasonNetwork || !latest.Streams.Terminal {
		t.Errorf("final observation = %+v, want Closed with final network reason and terminal streams", latest)
	}
	for _, event := range events {
		if event.State == connstats.StateClosed && event.CloseReason == connstats.ReasonNone {
			t.Error("terminal activation exposed an unset close reason")
		}
	}
}
