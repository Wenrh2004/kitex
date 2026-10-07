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
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/cloudwego/kitex/pkg/remote/connstats"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/grpc/grpcframe"
)

type connectionObserverFunc func(connstats.Meta) connstats.Recorder

func (f connectionObserverFunc) NewConnection(meta connstats.Meta) connstats.Recorder { return f(meta) }

type connectionRecorderFunc func(connstats.Event)

func (f connectionRecorderFunc) Observe(event connstats.Event) { f(event) }

type connectionEventRecorder struct {
	mu     sync.Mutex
	events []connstats.Event
	latest connstats.Event
}

func (r *connectionEventRecorder) Observe(event connstats.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
	if r.latest.State != connstats.StateClosed && event.Version > r.latest.Version {
		r.latest = event
	}
}

func (r *connectionEventRecorder) snapshot() (connstats.Event, []connstats.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.latest, append([]connstats.Event(nil), r.events...)
}

func TestConnectionObservationActivationAndTerminal(t *testing.T) {
	meta := connstats.Meta{Service: "service", Target: "proxy:123", Mode: connstats.ModeShort, PoolID: 7}
	r := new(connectionEventRecorder)
	var calls int
	var client *http2Client
	client = &http2Client{observation: newConnectionObservation(connectionObserverFunc(func(got connstats.Meta) connstats.Recorder {
		calls++
		if got != meta {
			t.Errorf("metadata = %+v, want %+v", got, meta)
		}
		// Reentrant state inspection demonstrates the factory is lock-free.
		client.IsActive()
		return connectionRecorderFunc(func(event connstats.Event) {
			client.IsActive() // Observe must also run without t.mu.
			r.Observe(event)
		})
	}), meta)}
	client.publishConnectionObservation()
	if _, events := r.snapshot(); len(events) != 0 {
		t.Fatal("observation was delivered before activation")
	}
	client.ActivateConnectionObserver()
	client.ActivateConnectionObserver()
	client.mu.Lock()
	client.observationTransitionLocked(connstats.StateDraining)
	client.observationTransitionLocked(connstats.StateDraining)
	client.mu.Unlock()
	client.publishConnectionObservation()
	client.mu.Lock()
	client.observationTransitionLocked(connstats.StateClosed)
	client.observationTransitionLocked(connstats.StateReachable)
	client.mu.Unlock()
	client.publishConnectionObservation()
	_, events := r.snapshot()
	want := []connstats.Event{{Version: 1, State: connstats.StateReachable}, {Version: 2, State: connstats.StateDraining}, {Version: 3, State: connstats.StateClosed}}
	if calls != 1 || !reflect.DeepEqual(events, want) {
		t.Fatalf("calls = %d, events = %v, want %v", calls, events, want)
	}
	if client.observation.factory != nil {
		t.Fatal("activated gate retains factory")
	}
}

func TestConnectionObservationFactoryConcurrentClose(t *testing.T) {
	r := new(connectionEventRecorder)
	entered, release, activated := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	client := &http2Client{observation: newConnectionObservation(connectionObserverFunc(func(connstats.Meta) connstats.Recorder {
		calls.Add(1)
		close(entered)
		<-release
		return r
	}), connstats.Meta{})}
	go func() {
		client.ActivateConnectionObserver()
		close(activated)
	}()
	awaitConnectionSignal(t, entered)
	// A repeated activation must not wait for the blocked factory.
	repeated := make(chan struct{})
	go func() { client.ActivateConnectionObserver(); close(repeated) }()
	awaitConnectionSignal(t, repeated)
	client.mu.Lock()
	client.observationTransitionLocked(connstats.StateDraining)
	client.observationTransitionLocked(connstats.StateClosed)
	client.mu.Unlock()
	client.publishConnectionObservation()
	close(release)
	awaitConnectionSignal(t, activated)
	latest, events := r.snapshot()
	if calls.Load() != 1 || latest != (connstats.Event{Version: 3, State: connstats.StateClosed}) || len(events) != 1 {
		t.Fatalf("calls = %d, latest = %v, events = %v", calls.Load(), latest, events)
	}
}

func TestConnectionObservationOutOfOrderDelivery(t *testing.T) {
	r := new(connectionEventRecorder)
	entered, release, activated := make(chan struct{}), make(chan struct{}), make(chan struct{})
	client := &http2Client{observation: newConnectionObservation(connectionObserverFunc(func(connstats.Meta) connstats.Recorder {
		return connectionRecorderFunc(func(event connstats.Event) {
			if event.Version == 1 {
				close(entered)
				<-release
			}
			r.Observe(event)
		})
	}), connstats.Meta{})}
	go func() { client.ActivateConnectionObserver(); close(activated) }()
	awaitConnectionSignal(t, entered)
	client.mu.Lock()
	client.observationTransitionLocked(connstats.StateClosed)
	client.mu.Unlock()
	client.publishConnectionObservation()
	close(release)
	awaitConnectionSignal(t, activated)
	latest, events := r.snapshot()
	if latest.State != connstats.StateClosed || len(events) != 2 || events[0].Version != 2 || events[1].Version != 1 {
		t.Fatalf("latest = %v, events = %v", latest, events)
	}
}

func TestConnectionObservationDisabledNilAndPanics(t *testing.T) {
	if o := newConnectionObservation(nil, connstats.Meta{}); o != nil {
		t.Fatal("disabled observation allocated a gate")
	}
	client := new(http2Client)
	client.ActivateConnectionObserver()
	client.publishConnectionObservation()
	client.observationTransitionLocked(connstats.StateClosed)
	for _, factory := range []connectionObserverFunc{
		func(connstats.Meta) connstats.Recorder { return nil },
		func(connstats.Meta) connstats.Recorder { panic("factory failure") },
		func(connstats.Meta) connstats.Recorder {
			return connectionRecorderFunc(func(connstats.Event) { panic("recorder failure") })
		},
	} {
		client := &http2Client{observation: newConnectionObservation(factory, connstats.Meta{})}
		client.ActivateConnectionObserver()
		client.mu.Lock()
		client.observationTransitionLocked(connstats.StateClosed)
		client.mu.Unlock()
		client.publishConnectionObservation()
	}
}

// This connection exercises real HTTP/2 construction, reader, and loopy writer
// without opening a socket. Reads deliver the server SETTINGS preface then wait
// for logical close; writes model a successfully writable established peer.
type connectionObserverTestConn struct {
	mu          sync.Mutex
	input       *bytes.Reader
	closed      chan struct{}
	closeOnce   sync.Once
	writeErr    error
	readEOF     bool
	beforeWrite func()
	headers     chan struct{}
}

func newConnectionObserverTestConn() *connectionObserverTestConn {
	var input bytes.Buffer
	_ = http2.NewFramer(&input, nil).WriteSettings()
	return &connectionObserverTestConn{input: bytes.NewReader(input.Bytes()), closed: make(chan struct{})}
}

func (c *connectionObserverTestConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	if c.input.Len() > 0 {
		n, err := c.input.Read(p)
		c.mu.Unlock()
		return n, err
	}
	eof := c.readEOF
	c.mu.Unlock()
	if !eof {
		<-c.closed
	}
	return 0, io.EOF
}

func (c *connectionObserverTestConn) Write(p []byte) (int, error) {
	if c.beforeWrite != nil {
		c.beforeWrite()
	}
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	if c.headers != nil && !bytes.HasPrefix(p, []byte(http2.ClientPreface)) {
		fr := http2.NewFramer(nil, bytes.NewReader(p))
		for {
			frame, err := fr.ReadFrame()
			if err != nil {
				break
			}
			if _, ok := frame.(*http2.HeadersFrame); ok {
				c.headers <- struct{}{}
			}
		}
	}
	return len(p), nil
}

func (c *connectionObserverTestConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (*connectionObserverTestConn) LocalAddr() net.Addr              { return connectionObserverTestAddr("local") }
func (*connectionObserverTestConn) RemoteAddr() net.Addr             { return connectionObserverTestAddr("remote") }
func (*connectionObserverTestConn) SetDeadline(time.Time) error      { return nil }
func (*connectionObserverTestConn) SetReadDeadline(time.Time) error  { return nil }
func (*connectionObserverTestConn) SetWriteDeadline(time.Time) error { return nil }

type connectionObserverTestAddr string

func (a connectionObserverTestAddr) Network() string { return "test" }
func (a connectionObserverTestAddr) String() string  { return string(a) }

func awaitConnectionSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for transport lifecycle")
	}
}

func newObservedTestTransport(t *testing.T, r connstats.Recorder) *http2Client {
	t.Helper()
	tr, err := NewClientTransportWithConfig(context.Background(), newConnectionObserverTestConn(), ConnectOptions{
		ConnectionObserver: connectionObserverFunc(func(connstats.Meta) connstats.Recorder { return r }),
	}, ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	client := tr.(*http2Client)
	t.Cleanup(func() {
		client.Close(ErrConnClosing)
		awaitConnectionSignal(t, client.readerDone)
		awaitConnectionSignal(t, client.writerDone)
	})
	return client
}

func TestConnectionObserverBlockingTerminalAfterCleanup(t *testing.T) {
	for _, operation := range []string{"close", "graceful-idle", "goaway-idle"} {
		t.Run(operation, func(t *testing.T) {
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			r := connectionRecorderFunc(func(event connstats.Event) {
				if event.State == connstats.StateClosed {
					once.Do(func() { close(entered); <-release })
				}
			})
			client := newObservedTestTransport(t, r)
			client.ActivateConnectionObserver()
			go func() {
				switch operation {
				case "close":
					_ = client.Close(ErrConnClosing)
				case "graceful-idle":
					client.GracefulClose()
				case "goaway-idle":
					client.handleGoAway(&grpcframe.GoAwayFrame{LastStreamID: 0})
				}
				close(done)
			}()
			awaitConnectionSignal(t, entered)
			// A deliberately contract-violating recorder may delay return, but
			// must not prevent resource release or reader shutdown.
			awaitConnectionSignal(t, client.ctx.Done())
			awaitConnectionSignal(t, client.conn.(*connectionObserverTestConn).closed)
			awaitConnectionSignal(t, client.readerDone)
			close(release)
			awaitConnectionSignal(t, done)
		})
	}
}

func TestConnectionObserverBlockingGoAwayAfterStreamCleanup(t *testing.T) {
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	client := newObservedTestTransport(t, connectionRecorderFunc(func(event connstats.Event) {
		if event.State == connstats.StateDraining {
			once.Do(func() { close(entered); <-release })
		}
	}))
	client.ActivateConnectionObserver()
	first, err := client.NewStream(context.Background(), &CallHdr{Method: "/service/method"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.NewStream(context.Background(), &CallHdr{Method: "/service/method"})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		client.handleGoAway(&grpcframe.GoAwayFrame{LastStreamID: first.id})
		close(done)
	}()
	awaitConnectionSignal(t, entered)
	awaitConnectionSignal(t, second.done)
	if !second.Unprocessed() {
		t.Error("GOAWAY rejected stream must be marked unprocessed before observation")
	}
	close(release)
	awaitConnectionSignal(t, done)
	client.CloseStream(first, nil)
}

func TestConnectionObserverTransportGracefulClose(t *testing.T) {
	r := new(connectionEventRecorder)
	client := newObservedTestTransport(t, r)
	client.ActivateConnectionObserver()
	stream, err := client.NewStream(context.Background(), &CallHdr{Host: "service", Method: "/service/method"})
	if err != nil {
		t.Fatal(err)
	}
	client.GracefulClose()
	client.GracefulClose()
	latest, _ := r.snapshot()
	if latest != (connstats.Event{Version: 2, State: connstats.StateDraining}) {
		t.Fatalf("with active stream: %v", latest)
	}
	client.CloseStream(stream, nil)
	awaitConnectionSignal(t, client.ctx.Done())
	awaitConnectionSignal(t, client.readerDone)
	latest, events := r.snapshot()
	if latest != (connstats.Event{Version: 3, State: connstats.StateClosed}) || len(events) != 3 {
		t.Fatalf("after draining: latest = %v, events = %v", latest, events)
	}
}

func TestConnectionObserverTransportGoAway(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "active"}[active], func(t *testing.T) {
			r := new(connectionEventRecorder)
			client := newObservedTestTransport(t, r)
			client.ActivateConnectionObserver()
			var stream *Stream
			if active {
				var err error
				stream, err = client.NewStream(context.Background(), &CallHdr{Method: "/service/method"})
				if err != nil {
					t.Fatal(err)
				}
			}
			client.handleGoAway(&grpcframe.GoAwayFrame{LastStreamID: 1, ErrCode: http2.ErrCodeNo})
			if active {
				client.handleGoAway(&grpcframe.GoAwayFrame{LastStreamID: 1, ErrCode: http2.ErrCodeNo})
				latest, _ := r.snapshot()
				if latest != (connstats.Event{Version: 2, State: connstats.StateDraining}) {
					t.Fatalf("GOAWAY with active stream: %v", latest)
				}
				client.CloseStream(stream, nil)
			}
			awaitConnectionSignal(t, client.ctx.Done())
			client.Close(ErrConnClosing)
			client.GracefulClose()
			latest, _ := r.snapshot()
			if latest != (connstats.Event{Version: 3, State: connstats.StateClosed}) {
				t.Fatalf("GOAWAY close: %v", latest)
			}
		})
	}
}

func TestConnectionObserverTransportConstruction(t *testing.T) {
	t.Run("failed", func(t *testing.T) {
		conn := newConnectionObserverTestConn()
		conn.writeErr = errors.New("preface write failed")
		var calls atomic.Int32
		closedTransport := make(chan ClientTransport, 1)
		tr, err := NewClientTransportWithConfig(context.Background(), conn, ConnectOptions{
			ConnectionObserver: connectionObserverFunc(func(connstats.Meta) connstats.Recorder { calls.Add(1); return nil }),
		}, ClientConfig{OnClose: func(_ context.Context, trans ClientTransport, _ error) { closedTransport <- trans }})
		// The existing wrapper returns a typed nil *http2Client on error.
		if err == nil || (tr != nil && tr.(*http2Client) != nil) || calls.Load() != 0 {
			t.Fatalf("transport = %v, error = %v, observer calls = %d", tr, err, calls.Load())
		}
		awaitConnectionSignal(t, (<-closedTransport).(*http2Client).readerDone)
	})
	t.Run("success after reader closed", func(t *testing.T) {
		conn := newConnectionObserverTestConn()
		conn.readEOF = true
		closed := make(chan struct{})
		conn.beforeWrite = func() { <-closed }
		r := new(connectionEventRecorder)
		tr, err := NewClientTransportWithConfig(context.Background(), conn, ConnectOptions{
			ConnectionObserver: connectionObserverFunc(func(connstats.Meta) connstats.Recorder { return r }),
		}, ClientConfig{OnClose: func(context.Context, ClientTransport, error) { close(closed) }})
		if err != nil {
			t.Fatal(err)
		}
		client := tr.(*http2Client)
		client.ActivateConnectionObserver()
		latest, events := r.snapshot()
		if latest != (connstats.Event{Version: 2, State: connstats.StateClosed}) || len(events) != 1 {
			t.Fatalf("already closed construction: latest = %v, events = %v", latest, events)
		}
		awaitConnectionSignal(t, client.readerDone)
		awaitConnectionSignal(t, client.writerDone)
	})
	t.Run("disabled", func(t *testing.T) {
		tr, err := NewClientTransportWithConfig(context.Background(), newConnectionObserverTestConn(), ConnectOptions{}, ClientConfig{})
		if err != nil {
			t.Fatal(err)
		}
		client := tr.(*http2Client)
		if client.observation != nil {
			t.Fatal("disabled constructor allocated a gate")
		}
		client.Close(ErrConnClosing)
		awaitConnectionSignal(t, client.readerDone)
		awaitConnectionSignal(t, client.writerDone)
	})
}

func TestConnectionObserverLegacyMetadata(t *testing.T) {
	var got connstats.Meta
	tr, err := NewClientTransport(context.Background(), newConnectionObserverTestConn(), ConnectOptions{
		ShortConn: true, ConnectionPoolID: 12,
		ConnectionObserver: connectionObserverFunc(func(meta connstats.Meta) connstats.Recorder { got = meta; return nil }),
	}, "legacy-service", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	client := tr.(*http2Client)
	client.ActivateConnectionObserver()
	want := connstats.Meta{Service: "legacy-service", Target: "remote", Mode: connstats.ModeShort, PoolID: 12}
	if got != want {
		t.Fatalf("metadata = %+v, want %+v", got, want)
	}
	client.Close(ErrConnClosing)
	awaitConnectionSignal(t, client.readerDone)
	awaitConnectionSignal(t, client.writerDone)
}

func TestConnectionObserverTransportCloseDuringFactory(t *testing.T) {
	r := new(connectionEventRecorder)
	entered, release, activated := make(chan struct{}), make(chan struct{}), make(chan struct{})
	tr, err := NewClientTransportWithConfig(context.Background(), newConnectionObserverTestConn(), ConnectOptions{
		ConnectionObserver: connectionObserverFunc(func(connstats.Meta) connstats.Recorder {
			close(entered)
			<-release
			return r
		}),
	}, ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	client := tr.(*http2Client)
	go func() { client.ActivateConnectionObserver(); close(activated) }()
	awaitConnectionSignal(t, entered)
	closed := make(chan struct{})
	go func() { client.Close(ErrConnClosing); close(closed) }()
	awaitConnectionSignal(t, closed)
	close(release)
	awaitConnectionSignal(t, activated)
	latest, events := r.snapshot()
	if latest != (connstats.Event{Version: 2, State: connstats.StateClosed}) || len(events) != 1 {
		t.Fatalf("close during factory: latest = %v, events = %v", latest, events)
	}
	awaitConnectionSignal(t, client.readerDone)
	awaitConnectionSignal(t, client.writerDone)
}

func TestConnectionObserverTransportConcurrentCloseAndGoAway(t *testing.T) {
	r := new(connectionEventRecorder)
	client := newObservedTestTransport(t, r)
	client.ActivateConnectionObserver()
	if _, err := client.NewStream(context.Background(), &CallHdr{Method: "/service/method"}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); client.GracefulClose() }()
	go func() {
		defer wg.Done()
		client.handleGoAway(&grpcframe.GoAwayFrame{LastStreamID: 1, ErrCode: http2.ErrCodeNo})
	}()
	go func() { defer wg.Done(); client.Close(ErrConnClosing) }()
	wg.Wait()
	latest, _ := r.snapshot()
	if latest.State != connstats.StateClosed || latest.Version < 2 || latest.Version > 3 {
		t.Fatalf("concurrent shutdown: latest = %v", latest)
	}
}

func BenchmarkConnectionObserverReuse(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(b *testing.B) {
			client := new(http2Client)
			if enabled {
				client.observation = newConnectionObservation(connectionObserverFunc(func(connstats.Meta) connstats.Recorder { return nil }), connstats.Meta{})
				client.ActivateConnectionObserver()
			}
			var transport ClientTransport = client
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if activator, ok := transport.(ConnectionObserverActivator); ok {
					activator.ActivateConnectionObserver()
				}
			}
		})
	}
}

func BenchmarkConnectionObserverState(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(b *testing.B) {
			client := new(http2Client)
			if enabled {
				client.observation = newConnectionObservation(connectionObserverFunc(func(connstats.Meta) connstats.Recorder {
					return connectionRecorderFunc(func(connstats.Event) {})
				}), connstats.Meta{})
				client.ActivateConnectionObserver()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				client.mu.Lock()
				client.observationTransitionLocked(connstats.StateDraining)
				client.mu.Unlock()
				client.publishConnectionObservation()
			}
		})
	}
}

func BenchmarkConnectionObserverTransportLifecycle(b *testing.B) {
	factory := connectionObserverFunc(func(connstats.Meta) connstats.Recorder {
		return connectionRecorderFunc(func(connstats.Event) {})
	})
	for _, enabled := range []bool{false, true} {
		b.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(b *testing.B) {
			opts := ConnectOptions{}
			if enabled {
				opts.ConnectionObserver = factory
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				client, err := newHTTP2Client(context.Background(), newConnectionObserverTestConn(), opts, ClientConfig{
					OnClose: noopOnClose, OnGoAway: noopOnGoAway,
				})
				if err != nil {
					b.Fatal(err)
				}
				client.ActivateConnectionObserver()
				client.Close(ErrConnClosing)
				// Keep the benchmark bounded: no accumulation of transport goroutines.
				<-client.readerDone
				<-client.writerDone
			}
		})
	}
}
