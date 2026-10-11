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
	"io"
	"net"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/gofunc"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/connstats"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/grpc"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/taskstats"
)

type constructionTaskObserver struct {
	mu      sync.Mutex
	records []*constructionTaskRecorder
	factory func()
	event   func(taskstats.Event)
	changed chan struct{}
}

func (o *constructionTaskObserver) NewTask(taskstats.Meta) taskstats.Recorder {
	if o.factory != nil {
		o.factory()
	}
	r := &constructionTaskRecorder{owner: o}
	o.mu.Lock()
	o.records = append(o.records, r)
	o.mu.Unlock()
	return r
}

type constructionTaskRecorder struct {
	owner  *constructionTaskObserver
	latest taskstats.Event // protected by owner.mu
}

func (r *constructionTaskRecorder) Observe(event taskstats.Event) {
	o := r.owner
	if o.event != nil {
		o.event(event)
	}
	o.mu.Lock()
	if event.Version > r.latest.Version && r.latest.State != taskstats.StateFinished {
		r.latest = event
	}
	o.mu.Unlock()
	if o.changed != nil {
		select {
		case o.changed <- struct{}{}:
		default:
		}
	}
}

func (o *constructionTaskObserver) waitFinished(t *testing.T, expected int) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		o.mu.Lock()
		finished := 0
		for _, r := range o.records {
			if r.latest.State == taskstats.StateFinished {
				finished++
			}
		}
		count := len(o.records)
		o.mu.Unlock()
		if count == expected && finished == expected {
			return
		}
		select {
		case <-o.changed:
		case <-timer.C:
			t.Fatalf("construction tasks: created=%d finished=%d, want %d", count, finished, expected)
		}
	}
}

func TestTaskConstructionObserverReentersPool(t *testing.T) {
	for _, phase := range []string{"factory", "event"} {
		t.Run(phase, func(t *testing.T) {
			defer gofunc.SetTaskObserver(nil)
			pool := observerPool(t, nil, false)
			opt := remote.ConnOption{Dialer: observerPipeDialer(t), ConnectTimeout: time.Second}
			var entered atomic.Bool
			nestedDone := make(chan error, 1)
			reenter := func() {
				if !entered.CompareAndSwap(false, true) {
					return
				}
				go func() {
					conn, err := pool.Get(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), "tcp", mockAddr0, opt)
					if err == nil {
						cc := conn.(*clientConn)
						cc.tr.CloseStream(cc.s, nil)
					}
					nestedDone <- err
				}()
				select {
				case err := <-nestedDone:
					if err != nil {
						t.Errorf("reentrant Get failed: %v", err)
					}
				case <-time.After(2 * time.Second):
					t.Error("reentrant Get waited on its own construction flight")
				}
			}
			o := &constructionTaskObserver{changed: make(chan struct{}, 8)}
			if phase == "factory" {
				o.factory = reenter
			} else {
				o.event = func(taskstats.Event) { reenter() }
			}
			gofunc.SetTaskObserver(o)
			cc := observerGet(t, pool, opt)
			if !entered.Load() {
				t.Fatal("regression fixture did not enter an actual observer hook")
			}
			cc.tr.CloseStream(cc.s, nil)
			_ = cc.tr.Close(errors.New("test cleanup"))
			o.waitFinished(t, 2)
		})
	}
}

func TestTaskConstructionCallbacksOutsideSlot(t *testing.T) {
	for _, global := range []bool{false, true} {
		t.Run(fmt.Sprintf("global=%v", global), func(t *testing.T) {
			defer gofunc.SetTaskObserver(nil)
			slot := new(transportSlot)
			t.Cleanup(slot.close)
			var checks atomic.Int32
			check := func() {
				checks.Add(1)
				if !slot.mu.TryLock() {
					t.Error("task observer hook ran under slot mutex")
					return
				}
				slot.mu.Unlock()
			}
			o := &constructionTaskObserver{factory: check, event: func(taskstats.Event) { check() }, changed: make(chan struct{}, 8)}
			if global {
				gofunc.SetTaskObserver(o)
			} else {
				original := gofunc.GoFunc
				gofunc.GoFunc = gofunc.WrapTask(original, o, taskstats.Meta{Executor: "local-wrapper"})
				defer func() { gofunc.GoFunc = original }()
			}
			tr, err := slot.createTransport("task-service", observerPipeDialer(t), "tcp", mockAddr0, time.Second, slotTestOpts)
			if err != nil {
				t.Fatal(err)
			}
			_ = tr.Close(errors.New("test cleanup"))
			o.waitFinished(t, 2)
			if checks.Load() < 4 {
				t.Fatalf("hook checks=%d, want two real factory and event entries", checks.Load())
			}
		})
	}
}

// A nil observer at construction entry does not prove that later submissions
// are unobserved: SetTaskObserver can enable observation before writer dispatch.
func TestTaskConstructionObserverEnabledBeforeWriterSubmission(t *testing.T) {
	gofunc.SetTaskObserver(nil)
	t.Cleanup(func() { gofunc.SetTaskObserver(nil) })
	slot := new(transportSlot)
	t.Cleanup(slot.close)
	var checks atomic.Int32
	check := func() {
		checks.Add(1)
		if !slot.mu.TryLock() {
			t.Error("newly enabled task observer ran under the construction mutex")
			return
		}
		slot.mu.Unlock()
	}
	observer := &constructionTaskObserver{
		factory: check,
		event:   func(taskstats.Event) { check() },
		changed: make(chan struct{}, 8),
	}
	conn := newMockNpConn(mockAddr0)
	var enabled atomic.Bool
	conn.WriteFunc = func(data []byte) (int, error) {
		if enabled.CompareAndSwap(false, true) {
			gofunc.SetTaskObserver(observer)
		}
		return len(data), nil
	}
	dialer := &remote.SynthesizedDialer{
		DialFunc: func(string, string, time.Duration) (net.Conn, error) { return conn, nil },
	}
	tr, err := slot.createTransport("task-service", dialer, "tcp", mockAddr0, time.Second, slotTestOpts)
	if err != nil {
		t.Fatal(err)
	}
	if !enabled.Load() {
		t.Fatal("fixture did not enable observation before writer submission")
	}
	_ = tr.Close(errors.New("test cleanup"))
	// The mock netpoll reader is not a GoFunc submission. Only the writer,
	// dispatched after the preface write enabled observation, is counted.
	observer.waitFinished(t, 1)
	if checks.Load() < 2 {
		t.Fatalf("observer hook checks=%d, want factory and event delivery", checks.Load())
	}
}

func TestTaskConstructionFailureAndDispatcherPanic(t *testing.T) {
	for _, failure := range []string{"preface", "settings", "flush", "first-submit", "keepalive-submit", "writer-submit"} {
		t.Run(failure, func(t *testing.T) {
			defer gofunc.SetTaskObserver(nil)
			original := gofunc.GoFunc
			defer func() { gofunc.GoFunc = original }()
			slot := new(transportSlot)
			t.Cleanup(slot.close)
			o := &constructionTaskObserver{changed: make(chan struct{}, 8), factory: func() {
				if !slot.mu.TryLock() {
					t.Error("failure activated factory before releasing slot")
					return
				}
				slot.mu.Unlock()
			}}
			gofunc.SetTaskObserver(o)
			var owned *constructionCloseConn
			var dialer remote.Dialer = newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) {
				conn, _ := newConnectionPipe(t)
				owned = &constructionCloseConn{Conn: conn, closed: make(chan struct{})}
				if failure == "preface" {
					return &constructionFailWriteConn{Conn: owned, failAt: 1}, nil
				}
				if failure == "settings" || failure == "flush" {
					return &constructionFailWriteConn{Conn: owned, failAt: 2}, nil
				}
				return owned, nil
			})
			opts := slotTestOpts
			opts.KeepaliveParams.Time = grpc.Infinity
			dialObserver := &poolDialObserver{}
			opts.ConnectionObserver = dialObserver
			expected := 1
			if failure == "settings" || failure == "flush" {
				opts.InitialConnWindowSize = 65535
				if failure == "settings" {
					opts.WriteBufferSize = 1
				} else {
					opts.WriteBufferSize = 64
				}
			}
			panics := strings.HasSuffix(failure, "submit")
			if panics {
				if failure == "keepalive-submit" {
					opts.KeepaliveParams.Time = time.Hour
				}
				calls := 0
				gofunc.GoFunc = func(_ context.Context, f func()) {
					calls++
					if failure == "first-submit" || calls == 2 {
						panic("dispatch")
					}
					go f()
				}
				if failure != "first-submit" {
					expected = 2
				}
			}
			func() {
				defer func() {
					recovered := recover()
					if panics && recovered != "dispatch" {
						t.Errorf("dispatcher panic changed: %v", recovered)
					}
				}()
				tr, err := slot.createTransport("task-service", dialer, "tcp", mockAddr0, time.Second, opts)
				if !panics && (tr != nil || err == nil) {
					t.Errorf("construction failure = (%v, %v)", tr, err)
				}
				if !panics {
					want := map[string]string{"preface": "failed to write client preface", "settings": "failed to write initial settings frame", "flush": "failed to Flush initial settings frame"}[failure]
					if !strings.Contains(fmt.Sprint(err), want) {
						t.Errorf("did not exercise %s failure: %v", failure, err)
					}
				}
			}()
			select {
			case <-owned.closed:
			case <-time.After(2 * time.Second):
				t.Fatal("failed constructor did not release its owned socket")
			}
			o.waitFinished(t, expected)
			dials, _ := dialObserver.snapshot()
			if len(dials) != 1 || dials[0].Outcome != connstats.DialTransportFailed {
				t.Fatalf("exception reported successful dial: %+v", dials)
			}
			if !slot.mu.TryLock() {
				t.Fatal("constructor exception retained slot mutex")
			}
			slot.mu.Unlock()
		})
	}
}

type constructionFailWriteConn struct {
	net.Conn
	failAt int
	writes int
}

type constructionCloseConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *constructionCloseConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.closed) })
	return err
}

func (c *constructionFailWriteConn) Write(p []byte) (int, error) {
	c.writes++
	if c.writes == c.failAt {
		return 0, io.ErrClosedPipe
	}
	return c.Conn.Write(p)
}

func TestTaskConstructionPanicLeavesSingleflight(t *testing.T) {
	defer gofunc.SetTaskObserver(nil)
	original := gofunc.GoFunc
	defer func() { gofunc.GoFunc = original }()
	pool := observerPool(t, nil, false)
	opt := remote.ConnOption{Dialer: observerPipeDialer(t), ConnectTimeout: time.Second}
	var once atomic.Bool
	nestedJoined := make(chan struct{})
	o := &constructionTaskObserver{changed: make(chan struct{}, 8), factory: func() {
		if !once.CompareAndSwap(false, true) {
			return
		}
		// The nested successful construction uses the normal dispatcher.
		gofunc.GoFunc = original
		nestedResult := make(chan error, 1)
		go func() {
			defer close(nestedJoined)
			conn, err := pool.Get(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), "tcp", mockAddr0, opt)
			if err == nil {
				cc := conn.(*clientConn)
				cc.tr.CloseStream(cc.s, nil)
				_ = cc.tr.Close(errors.New("nested cleanup"))
			}
			nestedResult <- err
		}()
		select {
		case err := <-nestedResult:
			if err != nil {
				t.Errorf("nested Get after panic failed: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("exceptional activation waited on its own flight")
		}
	}}
	gofunc.SetTaskObserver(o)
	gofunc.GoFunc = func(context.Context, func()) { panic("dispatch") }
	func() {
		defer func() {
			if p := recover(); !strings.Contains(fmt.Sprint(p), "dispatch") {
				t.Errorf("singleflight panic lost: %v", p)
			}
		}()
		_, _ = pool.Get(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), "tcp", mockAddr0, opt)
	}()
	if !once.Load() {
		t.Fatal("exceptional scope was not activated")
	}
	select {
	case <-nestedJoined:
	case <-time.After(2 * time.Second):
		t.Fatal("nested construction did not join")
	}
	o.waitFinished(t, 3)
}

func TestTaskConstructionSlowFactoryAllowsReaderExit(t *testing.T) {
	defer gofunc.SetTaskObserver(nil)
	slot := new(transportSlot)
	t.Cleanup(slot.close)
	var peer *connectionPipePeer
	o := &constructionTaskObserver{changed: make(chan struct{}, 8)}
	var entered atomic.Bool
	o.factory = func() {
		if !entered.CompareAndSwap(false, true) {
			return
		}
		tr := slot.load()
		if tr == nil {
			t.Error("factory ran before transport publication")
			return
		}
		_ = peer.conn.Close()
		select {
		case <-tr.Error():
		case <-time.After(2 * time.Second):
			t.Error("reader execution waited for factory activation")
		}
	}
	gofunc.SetTaskObserver(o)
	tr, err := slot.createTransport("task-service", observerPipeDialer(t, func(p *connectionPipePeer) { peer = p }), "tcp", mockAddr0, time.Second, slotTestOpts)
	if err != nil {
		t.Fatal(err)
	}
	_ = tr.Close(errors.New("test cleanup"))
	if !entered.Load() {
		t.Fatal("fixture did not enter factory")
	}
	o.waitFinished(t, 2)
}

func TestTaskConstructionCapturesObserverAndCloseOwnership(t *testing.T) {
	for _, closePool := range []bool{false, true} {
		t.Run(fmt.Sprintf("close=%v", closePool), func(t *testing.T) {
			defer gofunc.SetTaskObserver(nil)
			pool := observerPool(t, nil, false)
			first := &constructionTaskObserver{changed: make(chan struct{}, 8)}
			second := &constructionTaskObserver{changed: make(chan struct{}, 8)}
			var entered atomic.Bool
			first.factory = func() {
				if entered.CompareAndSwap(false, true) {
					gofunc.SetTaskObserver(second)
					if closePool {
						_ = pool.Close()
					}
				}
			}
			gofunc.SetTaskObserver(first)
			opt := remote.ConnOption{Dialer: observerPipeDialer(t), ConnectTimeout: time.Second}
			conn, err := pool.Get(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), "tcp", mockAddr0, opt)
			if closePool {
				if conn != nil || err != errConnPoolClosed {
					t.Fatalf("reentrant Close = (%v, %v)", conn, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				cc := conn.(*clientConn)
				cc.tr.CloseStream(cc.s, nil)
				_ = cc.tr.Close(errors.New("test cleanup"))
			}
			first.waitFinished(t, 2)
			second.mu.Lock()
			defer second.mu.Unlock()
			if len(second.records) != 0 {
				t.Fatal("activation reread global observer instead of capturing submissions")
			}
		})
	}
}

func TestTaskConstructionWorkerPaths(t *testing.T) {
	for _, path := range []string{"netpoll", "keepalive", "short", "wrapper-overflow"} {
		t.Run(path, func(t *testing.T) {
			defer gofunc.SetTaskObserver(nil)
			original := gofunc.GoFunc
			defer func() { gofunc.GoFunc = original }()
			o := &constructionTaskObserver{changed: make(chan struct{}, 64)}
			gofunc.SetTaskObserver(o)
			opts := slotTestOpts
			dialer := observerPipeDialer(t)
			expected := 2
			switch path {
			case "netpoll":
				dialer = newMockDialer()
				expected = 1 // SetOnRequest is not a global GoFunc submission.
			case "keepalive":
				opts.KeepaliveParams.Time = time.Hour
				expected = 3
			case "short":
				opts.ShortConn = true
			case "wrapper-overflow":
				gofunc.SetTaskObserver(nil)
				for i := 0; i < 17; i++ {
					gofunc.GoFunc = gofunc.WrapTask(gofunc.GoFunc, o, taskstats.Meta{Executor: "custom"})
				}
				expected = 16
			}
			tr, err := newTransport("task-service", dialer, "tcp", mockAddr0, time.Second, opts, nil, nil)
			if err != nil || tr == nil {
				t.Fatalf("%s transport did not execute under observation: (%v, %v)", path, tr, err)
			}
			_ = tr.Close(errors.New("test cleanup"))
			o.waitFinished(t, expected)
		})
	}
}

func TestTaskConstructionLegacyNilPanic(t *testing.T) {
	if runLegacyNilConstructionTest(t) {
		return
	}
	for _, exit := range []string{"nil-panic", "ordinary-panic", "panic-error", "goexit"} {
		t.Run(exit, func(t *testing.T) {
			defer gofunc.SetTaskObserver(nil)
			original := gofunc.GoFunc
			defer func() { gofunc.GoFunc = original }()
			pool := observerPool(t, nil, false)
			dialObserver := &poolDialObserver{}
			pool.connOpts.ConnectionObserver = dialObserver
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			var owned *constructionCloseConn
			var isCalls atomic.Int32
			panicError := &constructionPanicError{calls: &isCalls}
			dialer := newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) {
				conn, _ := newConnectionPipe(t)
				owned = &constructionCloseConn{Conn: conn, closed: make(chan struct{})}
				return owned, nil
			})
			gofunc.GoFunc = func(context.Context, func()) {
				close(entered)
				<-release
				switch exit {
				case "nil-panic":
					panic(nil)
				case "ordinary-panic":
					panic("dispatch")
				case "panic-error":
					panic(panicError)
				default:
					runtime.Goexit()
				}
			}
			o := &constructionTaskObserver{changed: make(chan struct{}, 8)}
			var factories atomic.Int32
			o.factory = func() {
				factories.Add(1)
				select {
				case <-owned.closed:
				default:
					t.Error("exceptional factory ran before socket cleanup")
				}
				// A fresh call on the same key must complete inside the hook;
				// it does not construct another transport or need a new task.
				nested := make(chan struct{})
				go func() {
					_, _, _ = pool.sfg.Do(mockAddr0, func() (interface{}, error) { return nil, nil })
					close(nested)
				}()
				select {
				case <-nested:
				case <-time.After(2 * time.Second):
					t.Error("exceptional factory ran before singleflight deletion")
				}
			}
			gofunc.SetTaskObserver(o)
			type callerExit struct {
				returned  bool
				continued bool
				panic     interface{}
			}
			const callers = 8
			results := make(chan callerExit, callers)
			opt := remote.ConnOption{Dialer: dialer, ConnectTimeout: time.Second}
			for i := 0; i < callers; i++ {
				go func() {
					var result callerExit
					defer func() { results <- result }()
					func() {
						defer func() { result.panic = recover() }()
						_, _ = pool.Get(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), "tcp", mockAddr0, opt)
						result.returned = true
					}()
					result.continued = true
				}()
			}
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("fixture did not enter real dispatcher")
			}
			waitConstructionFlightWaiters(t, callers-1)
			releaseOnce.Do(func() { close(release) })
			for i := 0; i < callers; i++ {
				var result callerExit
				select {
				case result = <-results:
				case <-time.After(2 * time.Second):
					t.Fatal("construction caller did not finish unwinding")
				}
				if result.returned {
					t.Error("exceptional Get returned normally")
				}
				switch exit {
				case "nil-panic":
					if !result.continued || result.panic != nil {
						t.Errorf("legacy panic(nil) changed: %+v", result)
					}
				case "ordinary-panic":
					if !result.continued || !strings.Contains(fmt.Sprint(result.panic), "dispatch") {
						t.Errorf("ordinary singleflight panic changed: %+v", result)
					}
				case "panic-error":
					wrapped, ok := result.panic.(interface{ Unwrap() error })
					if !result.continued || !ok || wrapped.Unwrap() != panicError || isCalls.Load() != 0 {
						t.Errorf("original panic error traversed/changed: %+v; Is calls=%d", result, isCalls.Load())
					}
				default:
					if result.continued || result.panic != nil {
						t.Errorf("Goexit converted into panic/return: %+v", result)
					}
				}
			}
			o.waitFinished(t, 1)
			if factories.Load() != 1 {
				t.Fatalf("shared exceptional scope activated %d times", factories.Load())
			}
			dials, _ := dialObserver.snapshot()
			if len(dials) != 1 || dials[0].Outcome != connstats.DialTransportFailed {
				t.Fatalf("exceptional dial outcome = %+v", dials)
			}
		})
	}
}

type constructionPanicError struct{ calls *atomic.Int32 }

func (*constructionPanicError) Error() string { return "dispatch error" }

func (e *constructionPanicError) Is(error) bool {
	e.calls.Add(1)
	panic("panic error Is must not run")
}

func waitConstructionFlightWaiters(t *testing.T, expected int) {
	t.Helper()
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	stack := make([]byte, 1<<20)
	for {
		n := runtime.Stack(stack, true)
		waiters := 0
		for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(goroutine, "singleflight.(*Group).Do(") && strings.Contains(goroutine, "sync.(*WaitGroup).Wait(") {
				waiters++
			}
		}
		if waiters >= expected {
			return
		}
		select {
		case <-timeout.C:
			t.Fatalf("shared flight waiters=%d, want %d before exceptional exit", waiters, expected)
		default:
			runtime.Gosched()
		}
	}
}

func runLegacyNilConstructionTest(t *testing.T) bool {
	t.Helper()
	const marker = "KITEX_LEGACY_NIL_CONSTRUCTION_TEST"
	if os.Getenv(marker) == "1" {
		return false
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.timeout=30s", "-test.v")
	if coverDir := flag.Lookup("test.gocoverdir"); coverDir != nil && coverDir.Value.String() != "" {
		cmd.Args = append(cmd.Args, "-test.gocoverdir="+coverDir.Value.String())
	}
	debug := os.Getenv("GODEBUG")
	if debug != "" {
		debug += ","
	}
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, "GODEBUG=") {
			cmd.Env = append(cmd.Env, env)
		}
	}
	// Only the subprocess selects legacy behavior; preserve other debug options.
	cmd.Env = append(cmd.Env, "GODEBUG="+debug+"panicnil=1", marker+"=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("legacy nil construction subprocess: %v\n%s", err, output)
	} else {
		t.Logf("legacy nil construction subprocess:\n%s", output)
	}
	return true
}
