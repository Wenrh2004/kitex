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
	"io"
	"math"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/connstats"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
)

func TestConnectionObserverShortConnInvalidURLCleanup(t *testing.T) {
	observer := &poolConnectionObserver{}
	p := observerPool(t, observer, true)
	ctx := newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional)
	if err := rpcinfo.AsMutableEndpointInfo(rpcinfo.GetRPCInfo(ctx).To()).SetTag(rpcinfo.HTTPURL, "://invalid"); err != nil {
		t.Fatal(err)
	}
	conn, err := p.Get(ctx, "tcp", mockAddr0, remote.ConnOption{Dialer: observerPipeDialer(t)})
	if err == nil || conn != nil {
		t.Fatalf("Get = (%v, %v), want wrapping error", conn, err)
	}
	records := observer.records()
	if len(records) != 1 {
		t.Fatalf("established transports = %d, want 1", len(records))
	}
	waitConnectionState(t, records[0], connstats.StateClosed)
}

func TestConnectionObserverShortConnCanceledQuotaCleanup(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
	acked, peerClosed := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(peerClosed)
		fr := http2.NewFramer(server, server)
		preface := make([]byte, len(http2.ClientPreface))
		if _, err := io.ReadFull(server, preface); err != nil {
			return
		}
		if _, err := fr.ReadFrame(); err != nil {
			return
		}
		if err := fr.WriteSettings(http2.Setting{ID: http2.SettingMaxConcurrentStreams, Val: 0}); err != nil {
			return
		}
		for {
			frame, err := fr.ReadFrame()
			if err != nil {
				return
			}
			if settings, ok := frame.(*http2.SettingsFrame); ok && settings.IsAck() {
				close(acked)
				break
			}
		}
		for {
			if _, err := fr.ReadFrame(); err != nil {
				return
			}
		}
	}()
	ctx, cancel := context.WithCancel(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional))
	t.Cleanup(func() {
		cancel()
		_ = client.Close()
		_ = server.Close()
		select {
		case <-peerClosed:
		case <-time.After(3 * time.Second):
			t.Error("quota test peer did not exit during cleanup")
		}
	})
	observer := &poolConnectionObserver{onNew: func(connstats.Meta) {
		select {
		case <-acked:
			cancel() // SETTINGS=0 has been processed, so NewStream must wait.
		case <-time.After(3 * time.Second):
			t.Error("SETTINGS ACK never arrived")
			cancel() // Also release NewStream when the fixture handshake failed.
		}
	}}
	p := observerPool(t, observer, true)
	conn, err := p.Get(ctx, "tcp", mockAddr0, remote.ConnOption{Dialer: newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) { return client, nil })})
	if err == nil || conn != nil {
		t.Fatalf("Get = (%v, %v), want canceled stream quota wait", conn, err)
	}
	records := observer.records()
	if len(records) != 1 {
		t.Fatalf("established transports = %d, want 1", len(records))
	}
	waitConnectionState(t, records[0], connstats.StateClosed)
	select {
	case <-peerClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("failed short connection kept its underlying socket open")
	}
}

type poolConnectionObserver struct {
	mu        sync.Mutex
	recorders []*poolConnectionRecorder
	onNew     func(connstats.Meta)
	onObserve func(connstats.Event)
}

type poolConnectionRecorder struct {
	mu     sync.Mutex
	meta   connstats.Meta
	latest connstats.Event
	events []connstats.Event
	hook   func(connstats.Event)
}

func (o *poolConnectionObserver) NewConnection(meta connstats.Meta) connstats.Recorder {
	r := &poolConnectionRecorder{meta: meta, hook: o.onObserve}
	o.mu.Lock()
	o.recorders = append(o.recorders, r)
	o.mu.Unlock()
	if o.onNew != nil {
		o.onNew(meta)
	}
	return r
}

func (r *poolConnectionRecorder) Observe(event connstats.Event) {
	r.mu.Lock()
	r.events = append(r.events, event)
	if event.Version > r.latest.Version {
		r.latest = event
	}
	r.mu.Unlock()
	if r.hook != nil {
		r.hook(event)
	}
}

func (o *poolConnectionObserver) records() []*poolConnectionRecorder {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]*poolConnectionRecorder(nil), o.recorders...)
}

func (r *poolConnectionRecorder) state() connstats.State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.latest.State
}

func waitConnectionState(t *testing.T, r *poolConnectionRecorder, state connstats.State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if r.state() == state {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("connection state = %v, want %v", r.state(), state)
}

func observerPool(t *testing.T, observer connstats.Observer, short bool) *connPool {
	t.Helper()
	opts := slotTestOpts
	opts.ShortConn = short
	opts.ConnectionObserver = observer
	p := NewConnPool("observed-service", 1, opts)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func observerGet(t *testing.T, p *connPool, opt remote.ConnOption) *clientConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), 2*time.Second)
	t.Cleanup(cancel)
	conn, err := p.Get(ctx, "tcp", mockAddr0, opt)
	if err != nil {
		t.Fatal(err)
	}
	cc := conn.(*clientConn)
	t.Cleanup(func() { _ = cc.tr.Close(errors.New("test cleanup")) })
	return cc
}

func TestConnectionObserverPoolMetadataAndReuse(t *testing.T) {
	observer := &poolConnectionObserver{}
	first := observerPool(t, observer, false)
	second := observerPool(t, observer, false)
	short := observerPool(t, observer, true)
	var peers []*connectionPipePeer
	opt := remote.ConnOption{Dialer: observerPipeDialer(t, func(peer *connectionPipePeer) { peers = append(peers, peer) }), ConnectTimeout: time.Second}
	a := observerGet(t, first, opt)
	waitConnectionHeader(t, peers[0])
	aAgain := observerGet(t, first, opt)
	waitConnectionHeader(t, peers[0])
	if a.tr != aAgain.tr {
		t.Fatal("size-one long pool did not reuse its transport")
	}
	b := observerGet(t, second, opt)
	waitConnectionHeader(t, peers[1])
	c := observerGet(t, short, opt)
	waitConnectionHeader(t, peers[2])
	records := observer.records()
	if len(records) != 3 {
		t.Fatalf("factory calls = %d, want 3 (reused transport must activate once)", len(records))
	}
	poolIDs := make(map[uint64]bool)
	for i, r := range records {
		wantMode := connstats.ModeLong
		if i == 2 {
			wantMode = connstats.ModeShort
		}
		if r.meta.Service != "observed-service" || r.meta.Target != mockAddr0 || r.meta.Mode != wantMode {
			t.Fatalf("record %d has unexpected metadata: %+v", i, r.meta)
		}
		if r.meta.PoolID == 0 || poolIDs[r.meta.PoolID] {
			t.Fatalf("pool identities must be nonzero and independent: %+v", r.meta)
		}
		poolIDs[r.meta.PoolID] = true
		waitConnectionState(t, r, connstats.StateReachable)
	}
	first.Clean("tcp", mockAddr0)
	waitConnectionState(t, records[0], connstats.StateDraining)
	a.tr.CloseStream(a.s, nil)
	a.tr.CloseStream(aAgain.s, nil)
	waitConnectionState(t, records[0], connstats.StateClosed)
	b.tr.CloseStream(b.s, nil)
	if err := short.Close(); err != nil {
		t.Fatal(err)
	}
	if records[2].state() != connstats.StateReachable {
		t.Fatal("short pool close must not forget a checked-out transport")
	}
	c.tr.CloseStream(c.s, nil)
	if err := short.Put(c); err != nil {
		t.Fatal(err)
	}
	waitConnectionState(t, records[2], connstats.StateClosed)
	if err := short.Discard(c); err != nil {
		t.Fatal(err)
	}
	if err := c.tr.Close(errors.New("duplicate close")); err != nil {
		t.Fatal(err)
	}
	records[2].mu.Lock()
	closedEvents := 0
	for _, event := range records[2].events {
		if event.State == connstats.StateClosed {
			closedEvents++
		}
	}
	records[2].mu.Unlock()
	if closedEvents != 1 {
		t.Fatalf("closed observations = %d, want exactly one", closedEvents)
	}
}

// connectionPipePeer reads the client preface and continuously consumes outgoing
// frames. Its write side stays open, so tests control when GOAWAY/EOF arrives.
type connectionPipePeer struct {
	conn   net.Conn
	write  sync.Mutex
	fr     *http2.Framer
	ready  chan struct{}
	header chan struct{}
	done   chan struct{}
}

func newConnectionPipe(t *testing.T) (net.Conn, *connectionPipePeer) {
	t.Helper()
	client, server := net.Pipe()
	peer := &connectionPipePeer{conn: server, fr: http2.NewFramer(server, server), ready: make(chan struct{}), header: make(chan struct{}, 4), done: make(chan struct{})}
	stop := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		_ = client.Close()
		_ = server.Close()
		select {
		case <-peer.done:
		case <-time.After(3 * time.Second):
			t.Error("connection test peer did not exit during cleanup")
		}
	})
	go func() {
		defer close(peer.done)
		preface := make([]byte, len(http2.ClientPreface))
		if _, err := io.ReadFull(server, preface); err != nil {
			return
		}
		if _, err := peer.fr.ReadFrame(); err != nil {
			return
		}
		peer.write.Lock()
		err := peer.fr.WriteSettings()
		peer.write.Unlock()
		if err != nil {
			return
		}
		close(peer.ready)
		for {
			frame, err := peer.fr.ReadFrame()
			if err != nil {
				return
			}
			if _, ok := frame.(*http2.HeadersFrame); ok {
				select {
				case peer.header <- struct{}{}:
				case <-stop:
					return
				}
			}
		}
	}()
	return client, peer
}

func observerPipeDialer(t *testing.T, onDial ...func(*connectionPipePeer)) remote.Dialer {
	t.Helper()
	return newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) {
		conn, peer := newConnectionPipe(t)
		for _, callback := range onDial {
			callback(peer)
		}
		return conn, nil
	})
}

func waitConnectionHeader(t *testing.T, peer *connectionPipePeer) {
	t.Helper()
	select {
	case <-peer.header:
	case <-time.After(2 * time.Second):
		t.Fatal("stream header was not written before draining")
	}
}

func TestConnectionObserverGoAwayOverlap(t *testing.T) {
	observer := &poolConnectionObserver{}
	pool := observerPool(t, observer, false)
	var peers []*connectionPipePeer
	dialer := newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) {
		conn, peer := newConnectionPipe(t)
		peers = append(peers, peer)
		return conn, nil
	})
	opt := remote.ConnOption{Dialer: dialer, ConnectTimeout: time.Second}
	old := observerGet(t, pool, opt)
	waitConnectionHeader(t, peers[0])
	select {
	case <-peers[0].ready:
	case <-time.After(2 * time.Second):
		t.Fatal("server preface was not consumed")
	}
	peers[0].write.Lock()
	err := peers[0].fr.WriteGoAway(math.MaxInt32, http2.ErrCodeNo, nil)
	peers[0].write.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	records := observer.records()
	waitConnectionState(t, records[0], connstats.StateDraining)
	v, ok := pool.conns.Load(mockAddr0)
	if !ok || len(v.(*transports).loadAll()) != 0 {
		t.Fatal("GOAWAY transport must leave the reachable pool")
	}
	current := observerGet(t, pool, opt)
	waitConnectionHeader(t, peers[1])
	if current.tr == old.tr {
		t.Fatal("GOAWAY must create a replacement transport")
	}
	records = observer.records()
	if len(records) != 2 {
		t.Fatalf("factory calls = %d, want 2", len(records))
	}
	waitConnectionState(t, records[1], connstats.StateReachable)
	if records[0].state() != connstats.StateDraining {
		t.Fatal("removed transport must remain counted as draining while its stream is active")
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	waitConnectionState(t, records[1], connstats.StateDraining)
	if records[0].state() != connstats.StateDraining {
		t.Fatal("pool close must not prematurely forget the removed draining transport")
	}
	old.tr.CloseStream(old.s, nil)
	waitConnectionState(t, records[0], connstats.StateClosed)
	current.tr.CloseStream(current.s, nil)
	waitConnectionState(t, records[1], connstats.StateClosed)
}

func TestConnectionObserverCallbacksOutsideSlotLock(t *testing.T) {
	ts := newTransports(1)
	t.Cleanup(ts.close)
	slot := ts.getTransportSlot(0)
	observer := &poolConnectionObserver{}
	checkUnlocked := func() {
		if !slot.mu.TryLock() {
			t.Error("observer callback ran while the transport slot mutex was held")
			return
		}
		slot.mu.Unlock()
		_ = ts.loadAll()
	}
	observer.onNew = func(connstats.Meta) { checkUnlocked() }
	observer.onObserve = func(connstats.Event) { checkUnlocked() }
	opts := slotTestOpts
	opts.ConnectionObserver = observer
	tr, err := ts.createTransport(0, "test", newMockDialer(), "tcp", mockAddr0, time.Second, opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ts.createTransport(0, "test", newMockDialer(), "tcp", mockAddr0, time.Second, opts); err != nil {
		t.Fatal(err)
	}
	if len(observer.records()) != 1 {
		t.Fatal("reuse must not create another recorder")
	}
	if err = tr.Close(errors.New("test close")); err != nil {
		t.Fatal(err)
	}
	waitConnectionState(t, observer.records()[0], connstats.StateClosed)
}

func TestConnectionObserverGetActiveTransportActivatesExistingSlot(t *testing.T) {
	ts := newTransports(1)
	t.Cleanup(ts.close)
	observer := &poolConnectionObserver{}
	opts := slotTestOpts
	opts.ConnectionObserver = observer
	tr, err := ts.getTransportSlot(0).createTransport("test", newMockDialer(), "tcp", mockAddr0, time.Second, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(observer.records()) != 0 {
		t.Fatal("slot creation must defer factory invocation until after returning")
	}
	for i := 0; i < 2; i++ {
		active, _ := ts.getActiveTransport()
		if active != tr {
			t.Fatal("getActiveTransport did not return the existing transport")
		}
	}
	if len(observer.records()) != 1 {
		t.Fatal("getActiveTransport must activate the existing transport exactly once")
	}
	waitConnectionState(t, observer.records()[0], connstats.StateReachable)
}

func TestConnectionObserverPoolCloseDuringActivation(t *testing.T) {
	observer := &poolConnectionObserver{}
	pool := observerPool(t, observer, false)
	observer.onNew = func(connstats.Meta) {
		// Address publication and singleflight completion precede activation.
		// Reentrant Close must still clean that entry and preserve Get's error.
		if _, ok := pool.conns.Load(mockAddr0); !ok {
			t.Error("factory ran before the address entry was published")
		}
		if err := pool.Close(); err != nil {
			t.Error(err)
		}
	}
	_, err := pool.Get(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), "tcp", mockAddr0,
		remote.ConnOption{Dialer: observerPipeDialer(t), ConnectTimeout: time.Second})
	if err != errConnPoolClosed {
		t.Fatalf("Get error = %v, want %v", err, errConnPoolClosed)
	}
	records := observer.records()
	if len(records) != 1 {
		t.Fatalf("successfully initialized transport must have one recorder, got %d", len(records))
	}
	waitConnectionState(t, records[0], connstats.StateClosed)
	pool.conns.Range(func(_, _ interface{}) bool {
		t.Error("reentrant pool close left an orphaned address entry")
		return false
	})
}

func TestConnectionObserverCallbacksOutsideSingleflight(t *testing.T) {
	for _, operation := range []string{"factory", "initial-state", "dial"} {
		t.Run(operation, func(t *testing.T) {
			observer := &poolDialObserver{}
			pool := observerPool(t, observer, false)
			opt := remote.ConnOption{Dialer: observerPipeDialer(t), ConnectTimeout: time.Second}
			var entered atomic.Bool
			reenter := func() {
				if !entered.CompareAndSwap(false, true) {
					return
				}
				done := make(chan error, 1)
				go func() {
					conn, err := pool.Get(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), "tcp", mockAddr0, opt)
					if err == nil {
						cc := conn.(*clientConn)
						cc.tr.CloseStream(cc.s, nil)
					}
					done <- err
				}()
				// A timeout breaks the old cycle so a failed regression cannot
				// leave Get or the suite permanently deadlocked.
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("reentrant Get: %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Error("same-address Get waited for its own observer callback")
				}
			}
			switch operation {
			case "factory":
				observer.onNew = func(connstats.Meta) { reenter() }
			case "initial-state":
				observer.onObserve = func(event connstats.Event) {
					if event.State == connstats.StateReachable {
						reenter()
					}
				}
			case "dial":
				observer.hook = reenter
			}
			cc := observerGet(t, pool, opt)
			cc.tr.CloseStream(cc.s, nil)
			if !entered.Load() || len(observer.records()) != 1 {
				t.Fatalf("callback entered = %v, recorder count = %d", entered.Load(), len(observer.records()))
			}
			dials, _ := observer.snapshot()
			if len(dials) != 1 || dials[0].Outcome != connstats.DialEstablished {
				t.Fatalf("reentrant transport reuse created extra dial events: %+v", dials)
			}
		})
	}
}

type observationCloseSignalConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (conn *observationCloseSignalConn) Close() error {
	err := conn.Conn.Close()
	conn.once.Do(func() { close(conn.closed) })
	return err
}

func TestConnectionObserverClosedPoolCleanupBeforeActivation(t *testing.T) {
	client, _ := newConnectionPipe(t)
	conn := &observationCloseSignalConn{Conn: client, closed: make(chan struct{})}
	observer := &poolDialObserver{}
	checkClosed := func() {
		select {
		case <-conn.closed:
		default:
			t.Error("observer ran before closed-pool socket cleanup")
		}
	}
	observer.onNew = func(connstats.Meta) { checkClosed() }
	observer.onObserve = func(event connstats.Event) {
		checkClosed()
		if event.State != connstats.StateClosed {
			t.Errorf("closed pool observed nonterminal state: %+v", event)
		}
	}
	observer.hook = checkClosed
	pool := observerPool(t, observer, false)
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Get(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), "tcp", mockAddr0,
		remote.ConnOption{Dialer: newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) {
			return conn, nil
		}), ConnectTimeout: time.Second})
	if err != errConnPoolClosed {
		t.Fatalf("Get error = %v, want %v", err, errConnPoolClosed)
	}
	if records := observer.records(); len(records) != 1 || records[0].state() != connstats.StateClosed {
		t.Fatalf("closed pool recorder states: %+v", records)
	}
	pool.conns.Range(func(_, _ interface{}) bool {
		t.Error("closed pool retained its newly dialed transport")
		return false
	})
}

func TestConnectionObserverSharedFailedDialPublishedOnce(t *testing.T) {
	const callers = 16
	dialStarted, releaseDial := make(chan struct{}), make(chan struct{})
	hookEntered, releaseHook := make(chan struct{}), make(chan struct{})
	var dialReleaseOnce, hookReleaseOnce sync.Once
	t.Cleanup(func() {
		dialReleaseOnce.Do(func() { close(releaseDial) })
		hookReleaseOnce.Do(func() { close(releaseHook) })
	})
	var attempts atomic.Int32
	wantErr := errors.New("shared failed dial")
	dialer := newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) {
		if attempts.Add(1) == 1 {
			close(dialStarted)
		}
		<-releaseDial
		return nil, wantErr
	})
	var hooks atomic.Int32
	observer := &poolDialObserver{hook: func() {
		if hooks.Add(1) == 1 {
			close(hookEntered)
		}
		<-releaseHook
	}}
	pool := observerPool(t, observer, false)
	done := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func() {
			_, err := pool.Get(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), "tcp", mockAddr0,
				remote.ConnOption{Dialer: dialer, ConnectTimeout: time.Second})
			done <- err
		}()
	}
	select {
	case <-dialStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("dial did not start")
	}
	// Wait for every real Get to join this flight, rather than depending on
	// scheduler timing or an arbitrary sleep before releasing the dial.
	deadline := time.Now().Add(3 * time.Second)
	stack := make([]byte, 1<<20)
	joined := false
	for time.Now().Before(deadline) {
		n := runtime.Stack(stack, true)
		if strings.Count(string(stack[:n]), "golang.org/x/sync/singleflight.(*Group).Do(") >= callers {
			joined = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !joined {
		t.Fatal("concurrent Get calls did not join the same singleflight")
	}
	dialReleaseOnce.Do(func() { close(releaseDial) })
	select {
	case <-hookEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("dial observation was not published")
	}
	// Publication is claimed once without a sync.Once wait. All other callers
	// must return even while the one publishing custom callback is blocked.
	for i := 0; i < callers-1; i++ {
		select {
		case err := <-done:
			if !errors.Is(err, wantErr) {
				t.Errorf("Get error = %v, want %v", err, wantErr)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("duplicate caller waited for a blocked observation publisher")
		}
	}
	hookReleaseOnce.Do(func() { close(releaseHook) })
	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Error(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("publishing Get did not finish")
	}
	dials, _ := observer.snapshot()
	if attempts.Load() != 1 || hooks.Load() != 1 || len(dials) != 1 || dials[0].Outcome != connstats.DialFailed || len(observer.records()) != 0 {
		t.Fatalf("attempts=%d hooks=%d dials=%+v recorders=%d", attempts.Load(), hooks.Load(), dials, len(observer.records()))
	}
}

func TestConnectionObserverSlotReplacementKeepsDrainingRecorder(t *testing.T) {
	ts := newTransports(1)
	t.Cleanup(ts.close)
	observer := &poolConnectionObserver{}
	opts := slotTestOpts
	opts.ConnectionObserver = observer
	var peers []*connectionPipePeer
	dialer := observerPipeDialer(t, func(peer *connectionPipePeer) { peers = append(peers, peer) })
	old, err := ts.createTransport(0, "test", dialer, "tcp", mockAddr0, time.Second, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = old.Close(errors.New("test cleanup")) })
	conn, err := newClientConn(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), old, mockAddr0)
	if err != nil {
		t.Fatal(err)
	}
	waitConnectionHeader(t, peers[0])
	old.GracefulClose()
	records := observer.records()
	waitConnectionState(t, records[0], connstats.StateDraining)
	current, err := ts.createTransport(0, "test", dialer, "tcp", mockAddr0, time.Second, opts)
	if err != nil {
		t.Fatal(err)
	}
	if current == old || ts.getTransportSlot(0).load() != current {
		t.Fatal("draining transport was not replaced in its slot")
	}
	records = observer.records()
	if len(records) != 2 || records[0].state() != connstats.StateDraining {
		t.Fatal("slot replacement must retain the old transport's draining recorder")
	}
	waitConnectionState(t, records[1], connstats.StateReachable)
	ts.getTransportSlot(0).removeTransport(old)
	if ts.getTransportSlot(0).load() != current {
		t.Fatal("late removal of the old transport removed its replacement")
	}
	old.CloseStream(conn.s, nil)
	waitConnectionState(t, records[0], connstats.StateClosed)
	if ts.getTransportSlot(0).load() != current {
		t.Fatal("old transport's onClose removed its replacement")
	}
}

func TestConnectionObserverConstructionFailure(t *testing.T) {
	observer := &poolConnectionObserver{}
	pool := observerPool(t, observer, false)
	wantErr := errors.New("dial failed")
	dialer := newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) {
		return nil, wantErr
	})
	_, err := pool.Get(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), "tcp", mockAddr0,
		remote.ConnOption{Dialer: dialer, ConnectTimeout: time.Second})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Get error = %v, want %v", err, wantErr)
	}
	if len(observer.records()) != 0 {
		t.Fatal("a failed dial must not register an established connection")
	}
	pool.conns.Range(func(_, _ interface{}) bool {
		t.Error("failed construction left a pool entry")
		return false
	})

	// A dial that succeeds but cannot send the HTTP/2 preface also must not
	// create a recorder for a transport that failed initialization.
	dialer = newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) {
		return &mockConn{
			RemoteAddrFunc: func() net.Addr { return connectionObserverAddr(mockAddr0) },
			WriteFunc:      func([]byte) (int, error) { return 0, wantErr },
		}, nil
	})
	_, err = pool.Get(newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional), "tcp", mockAddr0,
		remote.ConnOption{Dialer: dialer, ConnectTimeout: time.Second})
	if err == nil {
		t.Fatal("initialization with a failing preface write succeeded")
	}
	if len(observer.records()) != 0 {
		t.Fatal("a failed HTTP/2 initialization must not register an established connection")
	}
}

type connectionObserverAddr string

func (connectionObserverAddr) Network() string     { return "tcp" }
func (addr connectionObserverAddr) String() string { return string(addr) }

// This compares pool selection on an already activated transport. It isolates
// the observer's hot-path cost from dials, streams, RPC tracing and exporters.
func BenchmarkConnectionObserverPoolReuse(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		b.Run(name, func(b *testing.B) {
			ts := newTransports(1)
			b.Cleanup(ts.close)
			opts := slotTestOpts
			if enabled {
				opts.ConnectionObserver = &poolConnectionObserver{}
			}
			if _, err := ts.createTransport(0, "test", newMockDialer(), "tcp", mockAddr0, time.Second, opts); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if tr, _ := ts.getActiveTransport(); tr == nil {
					b.Fatal("established transport became inactive")
				}
			}
		})
	}
}
