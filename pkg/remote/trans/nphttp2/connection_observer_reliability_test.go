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
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/gofunc"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/taskstats"
)

type dialWarningLogger struct {
	klog.FullLogger
	calls  atomic.Int32
	value  interface{}
	panics bool
}

func (l *dialWarningLogger) Warnf(format string, args ...interface{}) {
	l.calls.Add(1)
	_ = fmt.Sprintf(format, args...)
	if l.panics {
		panic(l.value)
	}
}

type dialPanicValue struct{ formatted atomic.Int32 }

func (v *dialPanicValue) String() string {
	v.formatted.Add(1)
	return "arbitrary dial observer panic value"
}

func TestConnectionObserverDialWarningIsolation(t *testing.T) {
	if runIsolatedPoolWarningTest(t) {
		return
	}
	previous := klog.DefaultLogger()
	t.Cleanup(func() { klog.SetLogger(previous) })
	for _, nilPanic := range []bool{false, true} {
		for _, loggerPanics := range []bool{false, true} {
			t.Run(fmt.Sprintf("nil=%v/logger_panic=%v", nilPanic, loggerPanics), func(t *testing.T) {
				payload := new(dialPanicValue)
				var value interface{} = payload
				if nilPanic {
					value = nil
				}
				logger := &dialWarningLogger{FullLogger: previous, value: value, panics: loggerPanics}
				klog.SetLogger(logger)
				dialObserverLastWarning.Store(0)
				observer := &poolDialObserver{hook: func() { panic(value) }}
				opts := slotTestOpts
				opts.ConnectionObserver = observer
				sentinel := errors.New("original dial failure")
				tr, err := newTransport("service", newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) {
					return nil, sentinel
				}), "tcp", mockAddr0, time.Second, opts, nil, nil)
				if tr != nil || !errors.Is(err, sentinel) {
					t.Fatalf("observer or logger panic changed dial failure: transport = %v, error = %v", tr, err)
				}
				o := &transportDialObservation{observer: observer}
				o.publish()
				if logger.calls.Load() != 1 {
					t.Fatalf("warning calls = %d, want one limited warning", logger.calls.Load())
				}
				if payload.formatted.Load() != 0 {
					t.Fatal("warning invoked arbitrary panic payload formatter")
				}
			})
		}
	}
}

// Joining observed transport tasks ensures global test hooks remain installed
// until the real reader and writer have exited, including a failed ShortConn.
type reliabilityTaskObserver struct {
	mu        sync.Mutex
	recorders []*reliabilityTaskRecorder
}

type reliabilityTaskRecorder struct {
	once     sync.Once
	finished chan struct{}
}

func (o *reliabilityTaskObserver) NewTask(taskstats.Meta) taskstats.Recorder {
	r := &reliabilityTaskRecorder{finished: make(chan struct{})}
	o.mu.Lock()
	o.recorders = append(o.recorders, r)
	o.mu.Unlock()
	return r
}

func (r *reliabilityTaskRecorder) Observe(event taskstats.Event) {
	if event.State == taskstats.StateFinished {
		r.once.Do(func() { close(r.finished) })
	}
}

func (o *reliabilityTaskObserver) join(t *testing.T) {
	t.Helper()
	o.mu.Lock()
	recorders := append([]*reliabilityTaskRecorder(nil), o.recorders...)
	o.mu.Unlock()
	if len(recorders) != 2 {
		t.Errorf("observed transport workers = %d, want fallback reader and writer", len(recorders))
	}
	for _, r := range recorders {
		select {
		case <-r.finished:
		case <-time.After(3 * time.Second):
			t.Error("transport task did not exit during cleanup")
		}
	}
}

func TestConnectionObserverShortConnWarningPreservesOwnership(t *testing.T) {
	if runIsolatedPoolWarningTest(t) {
		return
	}
	for _, invalidURL := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream_failure=%v", invalidURL), func(t *testing.T) {
			previous := klog.DefaultLogger()
			logger := &dialWarningLogger{FullLogger: previous, panics: true, value: "logger panic"}
			klog.SetLogger(logger)
			t.Cleanup(func() { klog.SetLogger(previous) })
			tasks := new(reliabilityTaskObserver)
			gofunc.SetTaskObserver(tasks)
			t.Cleanup(func() { gofunc.SetTaskObserver(nil) })
			client, peer := newConnectionPipe(t)
			conn := &observationCloseSignalConn{Conn: client, closed: make(chan struct{})}
			observer := &poolDialObserver{hook: func() { panic("dial observer panic") }}
			pool := observerPool(t, observer, true)
			t.Cleanup(func() {
				_ = conn.Close()
				_ = peer.conn.Close()
				select {
				case <-peer.done:
				case <-time.After(3 * time.Second):
					t.Error("short connection test peer did not exit during cleanup")
				}
				tasks.join(t)
			})
			dialObserverLastWarning.Store(0)
			ctx := newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional)
			if invalidURL {
				if err := rpcinfo.AsMutableEndpointInfo(rpcinfo.GetRPCInfo(ctx).To()).SetTag(rpcinfo.HTTPURL, "://invalid"); err != nil {
					t.Fatal(err)
				}
			}
			got, err := pool.Get(ctx, "tcp", mockAddr0, remote.ConnOption{Dialer: newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) { return conn, nil })})
			if invalidURL {
				if err == nil || got != nil {
					t.Fatalf("failed stream returned (%v, %v)", got, err)
				}
				select {
				case <-conn.closed:
				case <-time.After(3 * time.Second):
					t.Fatal("failed short connection did not release its private transport")
				}
			} else {
				if err != nil || got == nil {
					t.Fatalf("successful short connection lost returned owner: (%v, %v)", got, err)
				}
				select {
				case <-conn.closed:
					t.Fatal("successful short connection was closed before owner handoff")
				default:
				}
				_ = got.(*clientConn).tr.Close(nil)
			}
			if logger.calls.Load() != 1 {
				t.Fatalf("dial warning calls = %d, want one", logger.calls.Load())
			}
		})
	}
}

func runIsolatedPoolWarningTest(t *testing.T) bool {
	t.Helper()
	const marker = "KITEX_POOL_OBSERVER_WARNING_TEST"
	if os.Getenv(marker) == t.Name() {
		return false
	}
	// klog.SetLogger is not concurrent-safe. Isolate it from writers created
	// by other package tests; the current binary preserves race instrumentation
	// and the environment preserves the selected panic(nil) runtime behavior.
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.timeout=25s")
	if coverDir := flag.Lookup("test.gocoverdir"); coverDir != nil && coverDir.Value.String() != "" {
		// Share counter pods with the parent without overwriting its profile.
		cmd.Args = append(cmd.Args, "-test.gocoverdir="+coverDir.Value.String())
	}
	cmd.Env = append(os.Environ(), marker+"="+t.Name())
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("isolated pool warning test: %v\n%s", err, output)
	}
	return true
}
