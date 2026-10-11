/*
 * Copyright 2021 CloudWeGo Authors
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
	"crypto/tls"
	"errors"
	"io"
	"net"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/cloudwego/kitex/internal/taskscope"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/connstats"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/codes"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/grpc"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/status"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

const (
	poolOpen   int32 = 0
	poolClosed int32 = 1
)

func poolSize() uint32 {
	// One connection per processor, and need redundancy。
	// Benchmark indicates this setting is the best performance.
	numP := runtime.GOMAXPROCS(0)
	return uint32(numP * 3 / 2)
}

// nextConnectionPoolID is only advanced for pools with observation enabled.
var nextConnectionPoolID uint64

// NewConnPool ...
func NewConnPool(remoteService string, size uint32, connOpts grpc.ConnectOptions) *connPool {
	if connOpts.ConnectionObserver != nil {
		connOpts.ConnectionPoolID = atomic.AddUint64(&nextConnectionPoolID, 1)
	}
	if connOpts.TraceController == nil {
		connOpts.TraceController = &rpcinfo.TraceController{}
	}
	// process Header and Trailer retrieving for unary
	connOpts.TraceController.AppendClientStreamEventHandler(unaryMetaEventHandler)
	if size == 0 {
		size = poolSize()
	}
	return &connPool{
		remoteService: remoteService,
		size:          size,
		connOpts:      connOpts,
	}
}

// connPool manages a pool of gRPC long connections.
type connPool struct {
	size          uint32
	sfg           singleflight.Group
	conns         sync.Map // key: address, value: *transports
	remoteService string   // remote service name
	connOpts      grpc.ConnectOptions
	closed        int32 // 1 means connPool has been closed
}

var (
	_                 remote.LongConnPool = (*connPool)(nil)
	errConnPoolClosed                     = status.Err(codes.Aborted, "connection pool has been closed")
	// singleflight v0.11 does not retain a legacy panic(nil). Bridge only that
	// value through its panicError's Unwrap, then restore it outside the flight.
	errNilTransportConstructionPanic = errors.New("legacy nil panic during transport construction")
)

// Get pick or generate a net.Conn and return
func (p *connPool) Get(ctx context.Context, network, address string, opt remote.ConnOption) (net.Conn, error) {
	if p.connOpts.ShortConn {
		return p.createShortConn(ctx, network, address, opt)
	}

	var (
		tr   grpc.ClientTransport
		idx  uint32
		conn *clientConn
		err  error
	)

	// there is no need to check whether connPool has been closed
	// because connPool would only be closed when Kitex Client is GCed
	v, ok := p.conns.Load(address)
	if ok {
		trans := v.(*transports)
		tr, idx = trans.getActiveTransport()
		if tr != nil {
			// Actually new a stream, reuse the connection (grpc.ClientTransport)
			conn, err = newClientConn(ctx, tr, address)
			if err == nil {
				return conn, nil
			}

			// when stream creations failed:
			// - gRPC Connection closed or draining: create a new connection
			// - ctx canceled: the request lifecycle has ended, exit immediately
			select {
			// ctx provided by users is canceled, we should not try to create a new gRPC connection
			case <-ctx.Done():
				return nil, err
			default:
			}
			klog.CtxDebugf(ctx, "KITEX: New grpc stream failed, network=%s, address=%s, error=%s", network, address, err.Error())
		}
	}
	rawTr, dErr := p.doTransportCreation(address, func(res *transportCreation) (interface{}, error) {
		var trans *transports
		var isNew bool
		// avoid creating duplicate transports
		if existTrans, ok := p.conns.Load(address); ok {
			trans = existTrans.(*transports)
		} else {
			trans = newTransports(p.size)
			isNew = true
		}

		cErr := trans.getTransportSlot(idx).createTransportInto(res, p.remoteService, opt.Dialer, network, address, opt.ConnectTimeout, p.connOpts)
		if cErr != nil {
			return res, cErr
		}

		if isNew {
			// Store first, then recheck closed state to eliminate TOCTOU:
			// if Close() finished its Range between our earlier check and this store,
			// self-clean here to prevent orphaned transports.
			p.conns.LoadOrStore(address, trans)
			if p.isClosed() {
				if recheckV, recheckOK := p.conns.LoadAndDelete(address); recheckOK {
					// Closing may publish observation. Finish it after releasing
					// singleflight so reentrant callbacks cannot wait on this call.
					res.closing = recheckV.(*transports)
				}
				return res, errConnPoolClosed
			}
		}

		return res, nil
	})
	creation, _ := rawTr.(*transportCreation)
	creation.finish(true)
	if dErr != nil {
		klog.CtxErrorf(ctx, "KITEX: New grpc client connection failed, network=%s, address=%s, error=%s", network, address, dErr.Error())
		return nil, dErr
	}
	// A factory may close the pool reentrantly after the entry is published.
	// Preserve the pool-close error and never create a stream on that transport.
	if p.isClosed() {
		return nil, errConnPoolClosed
	}
	klog.CtxDebugf(ctx, "KITEX: New grpc client connection succeed, network=%s, address=%s", network, address)
	return newClientConn(ctx, creation.transport, address)
}

// doTransportCreation installs the result before dispatching any task. Its
// deferred finish runs after singleflight removes the call and slot ownership
// releases its mutex. Only the leader owns an exceptional construction result.
func (p *connPool) doTransportCreation(address string, fn func(*transportCreation) (interface{}, error)) (interface{}, error) {
	var interrupted *transportCreation
	defer func() {
		recovered := recover()
		interrupted.finish(false)
		if recovered != nil {
			// singleflight's panicError unwraps exactly to the original panic
			// value. Avoid traversing user error Is/Unwrap methods here.
			if err, ok := recovered.(interface{ Unwrap() error }); ok && err.Unwrap() == errNilTransportConstructionPanic {
				panic(nil)
			}
			panic(recovered)
		}
	}()
	result, err, _ := p.sfg.Do(address, func() (interface{}, error) {
		creation := &transportCreation{}
		interrupted = creation
		return bridgeNilConstructionPanic(func() (interface{}, error) { return fn(creation) })
	})
	return result, err
}

func bridgeNilConstructionPanic(fn func() (interface{}, error)) (result interface{}, err error) {
	returned := false
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				// Preserve ordinary panic values and their existing singleflight
				// treatment. Only a recovered legacy nil panic reaches below.
				panic(recovered)
			}
		}()
		result, err = fn()
		returned = true
	}()
	if !returned {
		// Goexit continues unwinding its goroutine, so never reaches here.
		panic(errNilTransportConstructionPanic)
	}
	return result, err
}

// Put implements the ConnPool interface.
func (p *connPool) Put(conn net.Conn) error {
	if p.connOpts.ShortConn {
		return p.release(conn)
	}
	return nil
}

func (p *connPool) release(conn net.Conn) error {
	clientConn := conn.(*clientConn)
	clientConn.tr.GracefulClose()
	return nil
}

func (p *connPool) createShortConn(ctx context.Context, network, address string, opt remote.ConnOption) (net.Conn, error) {
	tr, err := newTransport(p.remoteService, opt.Dialer, network, address, opt.ConnectTimeout, p.connOpts, nil, nil)
	if err != nil {
		return nil, err
	}
	activateConnectionObserver(tr)
	conn, err := newClientConn(ctx, tr, address)
	if err != nil {
		// Short connections are owned exclusively by this call and never enter
		// the pool. No caller can release the transport when stream creation
		// fails, so release it here before returning the original error.
		_ = tr.Close(err)
		return nil, err
	}
	return conn, nil
}

// Discard implements the ConnPool interface.
func (p *connPool) Discard(conn net.Conn) error {
	if p.connOpts.ShortConn {
		return p.release(conn)
	}
	return nil
}

// Clean implements the LongConnPool interface.
func (p *connPool) Clean(network, address string) {
	if v, ok := p.conns.LoadAndDelete(address); ok {
		v.(*transports).close()
	}
}

// Close is to release resource of ConnPool, it is executed when client is closed.
func (p *connPool) Close() error {
	if !p.casClosed() {
		return nil
	}

	p.conns.Range(func(addr, trans interface{}) bool {
		p.conns.Delete(addr)
		trans.(*transports).close()
		return true
	})
	return nil
}

func (p *connPool) isClosed() bool {
	return atomic.LoadInt32(&p.closed) == poolClosed
}

func (p *connPool) casClosed() bool {
	return atomic.CompareAndSwapInt32(&p.closed, poolOpen, poolClosed)
}

type dumpEntry struct {
	addr string
	tr   grpc.ClientTransport
}

// Dump dumps the connection pool with the details of the underlying transport.
func (p *connPool) Dump() interface{} {
	defer func() {
		if panicErr := recover(); panicErr != nil {
			klog.Errorf("KITEX: dump gRPC client connection pool panic, err=%v, stack=%s", panicErr, string(debug.Stack()))
		}
	}()

	// remoteAddress -> []clientTransport, where each clientTransport is a connection. Distinguish the connection via localAddress.
	// If mesh egress is not enabled, toAddr should be the address of the callee service.
	// Otherwise, toAddr will be the same, so you should check the remoteAddress in each stream, which is read from the header.

	// sync.Map does not expose its length directly.
	// Since dump is a cold-path operation, performance is not a major concern here
	poolDump := make(map[string]interface{}, p.size)
	var cliTransDumps []dumpEntry
	p.conns.Range(func(k, v interface{}) bool {
		addr := k.(string)
		for _, tr := range v.(*transports).loadAll() {
			cliTransDumps = append(cliTransDumps, dumpEntry{addr: addr, tr: tr})
		}
		return true
	})

	for _, cliTransDump := range cliTransDumps {
		dumper, ok := cliTransDump.tr.(interface{ Dump() interface{} })
		if !ok {
			continue
		}
		var curr []interface{}
		if poolDump[cliTransDump.addr] == nil {
			curr = make([]interface{}, 0)
		} else {
			curr = poolDump[cliTransDump.addr].([]interface{})
		}
		curr = append(curr, dumper.Dump())
		poolDump[cliTransDump.addr] = curr
	}
	return poolDump
}

// newTransport creates a gRPC connection
func newTransport(remoteService string,
	dialer remote.Dialer, network, address string, connectTimeout time.Duration, opts grpc.ConnectOptions,
	onGoAway func(context.Context, grpc.ClientTransport, grpc.GoAwayReason),
	onClose func(context.Context, grpc.ClientTransport, error),
) (grpc.ClientTransport, error) {
	creation := &transportCreation{}
	defer creation.finish(false)
	return newTransportWithDialObservation(creation, remoteService, dialer, network, address, connectTimeout, opts, onGoAway, onClose)
}

// The caller owning a slot lock defers delivery until after unlocking. Dial
// tracking describes only real attempts, never reuse or pool rejection.
func newTransportWithDialObservation(creation *transportCreation, remoteService string,
	dialer remote.Dialer, network, address string, connectTimeout time.Duration, opts grpc.ConnectOptions,
	onGoAway func(context.Context, grpc.ClientTransport, grpc.GoAwayReason),
	onClose func(context.Context, grpc.ClientTransport, error),
) (tr grpc.ClientTransport, err error) {
	mode := connstats.ModeLong
	if opts.ShortConn {
		mode = connstats.ModeShort
	}
	meta := connstats.Meta{Service: remoteService, Target: address, Mode: mode, PoolID: opts.ConnectionPoolID}
	var observation *transportDialObservation
	if observer, ok := opts.ConnectionObserver.(connstats.DialObserver); ok {
		observation = &transportDialObservation{observer: observer, meta: meta}
		creation.dial = observation
		started := time.Now()
		defer func() { observation.event.Duration = time.Since(started) }()
	}
	constructed := false
	stage := connstats.DialFailed
	defer func() {
		// An exceptional Dialer/TLS/dispatcher exit has no ordinary error
		// return to classify. Never publish the zero-valued Established outcome
		// for it, and preserve any failure already classified below.
		if observation != nil && !constructed && observation.event.Outcome == connstats.DialEstablished {
			observation.event.Outcome = stage
			observation.event.Reason = connstats.ReasonOther
		}
	}()
	conn, err := dialer.DialTimeout(network, address, connectTimeout)
	if err != nil {
		observation.failed(connstats.DialFailed, err)
		return nil, err
	}
	rawOwned := true
	defer func() {
		if rawOwned {
			// This owner covers TLS/context preparation exceptions. Once grpc
			// construction begins, that constructor owns all cleanup paths.
			_ = conn.Close()
		}
	}()
	if opts.TLSConfig != nil {
		stage = connstats.DialTLSFailed
		tlsConn, tErr := newTLSConn(conn, opts.TLSConfig)
		if tErr != nil {
			// release tls handshake failed connection
			cErr := conn.Close()
			rawOwned = false
			if cErr != nil {
				klog.Warnf("KITEX: Close TLS handshake failed connection, err: %v", cErr)
			}
			observation.failed(connstats.DialTLSFailed, tErr)
			return nil, tErr
		}
		conn = tlsConn
	}
	constructionCtx, scope := taskscope.NewContext(context.Background())
	creation.tasks = scope
	stage = connstats.DialTransportFailed
	rawOwned = false
	tr, err = grpc.NewClientTransportWithConfig(
		constructionCtx, // transport lifetime is independent of the RPC context
		conn,
		opts,
		grpc.ClientConfig{
			RemoteService:  remoteService,
			OnGoAway:       onGoAway,
			OnClose:        onClose,
			ConnectionMeta: meta,
		},
	)
	if err != nil {
		observation.failed(connstats.DialTransportFailed, err)
	}
	constructed = err == nil
	return tr, err
}

type transportDialObservation struct {
	observer connstats.DialObserver
	meta     connstats.Meta
	event    connstats.DialEvent
	failure  error // retained only until publication outside slot locks and singleflight
}

var dialObserverLastWarning atomic.Int64

func (o *transportDialObservation) failed(outcome connstats.DialOutcome, err error) {
	if o == nil {
		return
	}
	o.event.Outcome = outcome
	o.event.Reason = connstats.ReasonOther
	o.failure = err
}

func safeDialFailureReason(outcome connstats.DialOutcome, err error) (reason connstats.Reason) {
	reason = connstats.ReasonOther
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			reason = connstats.ReasonOther
			warnConnPoolObserverPanic("DialReason")
		}
	}()
	reason = classifyDialFailure(outcome, err)
	returned = true
	return reason
}

func classifyDialFailure(outcome connstats.DialOutcome, err error) connstats.Reason {
	if connectionErr, ok := err.(grpc.ConnectionError); ok {
		err = connectionErr.Origin()
	}
	switch {
	case errors.Is(err, context.Canceled):
		return connstats.ReasonCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return connstats.ReasonDeadline
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrClosedPipe), errors.Is(err, net.ErrClosed):
		return connstats.ReasonNetwork
	default:
		var networkError net.Error
		if errors.As(err, &networkError) {
			if networkError.Timeout() {
				return connstats.ReasonDeadline
			}
			return connstats.ReasonNetwork
		} else if outcome == connstats.DialTLSFailed || outcome == connstats.DialTransportFailed {
			return connstats.ReasonProtocol
		} else {
			return connstats.ReasonOther
		}
	}
}

func (o *transportDialObservation) publish() {
	if o == nil {
		return
	}
	failure := o.failure
	o.failure = nil
	event := o.event
	// Classifiers may call Goexit. Preserve that exit while still reporting the
	// completed attempt, with the fallback reason already recorded by failed.
	defer func() { o.publishEvent(event) }()
	if failure != nil {
		event.Reason = safeDialFailureReason(event.Outcome, failure)
	}
}

func (o *transportDialObservation) publishEvent(event connstats.DialEvent) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			warnConnPoolObserverPanic("ObserveDial")
		}
	}()
	o.observer.ObserveDial(o.meta, event)
	returned = true
}

func warnConnPoolObserverPanic(phase string) {
	defer func() { _ = recover() }()
	now := time.Now().UnixNano()
	previous := dialObserverLastWarning.Load()
	if (previous == 0 || now-previous >= int64(time.Minute)) && dialObserverLastWarning.CompareAndSwap(previous, now) {
		klog.Warnf("KITEX: gRPC connection observer %s panic; observation may be incomplete (warnings limited to once per minute)", phase)
	}
}

// activateConnectionObserver must run after releasing transport creation locks.
// Optional capability keeps existing ClientTransport implementations compatible.
func activateConnectionObserver(tr grpc.ClientTransport) {
	if activator, ok := tr.(grpc.ConnectionObserverActivator); ok {
		activator.ActivateConnectionObserver()
	}
}

// newTLSConn constructs a client-side TLS connection and performs handshake.
func newTLSConn(conn net.Conn, tlsCfg *tls.Config) (net.Conn, error) {
	tlsConn := tls.Client(conn, tlsCfg)
	if err := tlsConn.Handshake(); err != nil {
		return nil, err
	}
	return tlsConn, nil
}

func checkActive(trans grpc.ClientTransport) bool {
	if trans == nil {
		return false
	}
	// grpc.ClientTransport is implemented by *http2Client in pkg/remote/trans/nphttp2/grpc
	// it implements grpc.IsActive
	return trans.(grpc.IsActive).IsActive()
}
