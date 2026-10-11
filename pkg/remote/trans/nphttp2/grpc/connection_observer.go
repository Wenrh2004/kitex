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
	"io"
	"math"
	"net"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote/connstats"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/codes"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/status"
)

// The observation pointer is initialized before starting the transport reader
// and never replaced. Except activationStarted, all fields use http2Client.mu.
// Construction records states without invoking user code; pool owners activate
// successfully constructed transports only after releasing their own locks.
type connectionObservation struct {
	factory           connstats.Observer
	meta              connstats.Meta
	activationStarted uint32
	recorder          connstats.Recorder
	latest            connstats.Event
	closePending      bool  // resource cleanup or final close reason classification is in progress
	closeError        error // retained only for deferred classification at first activation
	metricsEnabled    bool
	streams           connstats.StreamEvent
	streamAttempted   uint64 // highest snapshot handed to a recorder, not guaranteed delivery
	waits             map[*quotaWaitObservation]struct{}
}

type quotaWaitObservation struct {
	started   time.Time
	completed bool // protected by http2Client.mu
}

func newConnectionObservation(factory connstats.Observer, meta connstats.Meta) *connectionObservation {
	if factory == nil {
		return nil
	}
	o := &connectionObservation{
		factory: factory,
		meta:    meta,
		latest:  connstats.Event{Version: 1, State: connstats.StateReachable},
	}
	if _, ok := factory.(connstats.MetricsObserver); ok {
		o.metricsEnabled = true
		o.latest.EstablishedAt = time.Now()
		o.streams = connstats.StreamEvent{Version: 1, Capacity: defaultMaxStreamsClient}
	}
	return o
}

// observationTransitionLocked commits an absolute state alongside the actual
// transport state. The caller holds t.mu; closed is terminal.
func (t *http2Client) observationTransitionLocked(state connstats.State) {
	o := t.observation
	if o == nil || o.latest.State == state || o.latest.State == connstats.StateClosed {
		return
	}
	o.latest.Version++
	o.latest.State = state
	if o.metricsEnabled {
		now := time.Now()
		switch state {
		case connstats.StateDraining:
			o.latest.DrainingAt = now
		case connstats.StateClosed:
			o.latest.ClosedAt = now
			for wait := range o.waits {
				o.completeQuotaWaitLocked(wait, now)
			}
			o.waits = nil
			o.streams.Version++
			o.streams.Active, o.streams.Waiting, o.streams.Capacity = 0, 0, 0
			o.streams.Terminal = true
		}
	}
}

// publishConnectionObservation must be called without transport or pool locks.
// Delivery may race another publish or activation; recorders use the version to
// discard stale events. Disabled observation adds no lock acquisition.
func (t *http2Client) publishConnectionObservation() {
	if t.observation == nil {
		return
	}
	t.mu.Lock()
	if t.observation.closePending {
		t.mu.Unlock()
		return
	}
	r, event := t.observation.recorder, t.observation.latest
	event.Streams = t.observation.streams
	t.mu.Unlock()
	if r != nil {
		safeObserveConnection(r, event)
	}
}

// ActivateConnectionObserver implements the optional activation capability.
// An activation already in progress never blocks a repeated caller. Close and
// GOAWAY may continue while the factory runs; its recorder receives the latest
// committed state, even when construction succeeded on an already closed peer.
func (t *http2Client) ActivateConnectionObserver() {
	o := t.observation
	if o == nil || atomic.LoadUint32(&o.activationStarted) != 0 ||
		!atomic.CompareAndSwapUint32(&o.activationStarted, 0, 1) {
		return
	}
	t.mu.Lock()
	factory, meta := o.factory, o.meta
	o.factory = nil
	t.mu.Unlock()
	r := safeNewConnection(factory, meta)
	t.mu.Lock()
	o.recorder = r
	if r == nil {
		o.closeError = nil
	}
	if o.closePending {
		t.mu.Unlock()
		return
	}
	if o.latest.State == connstats.StateClosed {
		// Close may have completed before activation, including while transport
		// construction held a pool lock. Classify its error only at this boundary.
		o.closePending = true
		t.mu.Unlock()
		t.finishCloseObservation()
		return
	}
	event := o.latest
	event.Streams = o.streams
	t.mu.Unlock()
	if r != nil {
		safeObserveConnection(r, event)
	}
}

// finishCloseObservation runs after physical cleanup, or at the first activation
// of an already closed transport. Only those two owners reach this boundary;
// closePending prevents either owner from publishing an incomplete terminal.
func (t *http2Client) finishCloseObservation() {
	t.mu.Lock()
	o := t.observation
	if o == nil {
		t.mu.Unlock()
		return
	}
	if !o.metricsEnabled || o.recorder == nil || o.latest.CloseReason != connstats.ReasonNone {
		if !o.metricsEnabled || o.latest.CloseReason != connstats.ReasonNone {
			o.closeError = nil
		}
		o.closePending = false
		t.mu.Unlock()
		t.publishConnectionObservation()
		return
	}
	err := o.closeError
	o.closeError = nil
	t.mu.Unlock()
	reason := connstats.ReasonOther
	// An error method may call Goexit. Keep that exit while still delivering the
	// complete logical terminal with its original close time and stream totals.
	defer func() {
		t.mu.Lock()
		if o.latest.GoAwayReceived != 0 {
			reason = connstats.ReasonGoAway
		}
		o.latest.CloseReason = reason
		o.closePending = false
		t.mu.Unlock()
		t.publishConnectionObservation()
	}()
	reason = safeConnectionCloseReason(err)
}

// Called while holding t.mu, alongside stream insertion. The check is kept
// local so state-only observers do not acquire an additional lock or clock read.
func (t *http2Client) observationStreamOpenedLocked() {
	if o := t.observation; o != nil && o.metricsEnabled && !o.streams.Terminal {
		o.streams.Active++
		o.streams.Version++
	}
}

func (t *http2Client) observationStreamClosed() {
	if t.observation == nil || !t.observation.metricsEnabled {
		return
	}
	t.mu.Lock()
	if o := t.observation; !o.streams.Terminal && o.streams.Active > 0 {
		o.streams.Active--
		o.streams.Version++
	}
	t.mu.Unlock()
}

func (t *http2Client) observationQuotaWaitStarted() *quotaWaitObservation {
	if t.observation == nil || !t.observation.metricsEnabled {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	o := t.observation
	if o.streams.Terminal {
		return nil
	}
	wait := &quotaWaitObservation{started: time.Now()}
	if o.waits == nil {
		o.waits = make(map[*quotaWaitObservation]struct{})
	}
	o.waits[wait] = struct{}{}
	o.streams.Waiting++
	o.streams.Version++
	return wait
}

func (o *connectionObservation) completeQuotaWaitLocked(wait *quotaWaitObservation, now time.Time) {
	if wait.completed {
		return
	}
	wait.completed = true
	delete(o.waits, wait)
	if o.streams.Waiting > 0 {
		o.streams.Waiting--
	}
	o.streams.QuotaWaitCount++
	elapsed := now.Sub(wait.started)
	// Duration is a signed nanosecond counter. Preserve monotonicity even
	// after an extreme aggregate lifetime (roughly 292 years of wait time).
	if elapsed > time.Duration(math.MaxInt64)-o.streams.QuotaWaitDuration {
		o.streams.QuotaWaitDuration = time.Duration(math.MaxInt64)
	} else if elapsed > 0 {
		o.streams.QuotaWaitDuration += elapsed
	}
	o.streams.Version++
}

func (t *http2Client) observationQuotaWaitEnded(wait *quotaWaitObservation) {
	if wait == nil {
		return
	}
	t.mu.Lock()
	if !wait.completed {
		t.observation.completeQuotaWaitLocked(wait, time.Now())
	}
	t.mu.Unlock()
	t.publishStreamObservation()
}

func (t *http2Client) observationCapacity(capacity uint32) {
	if t.observation == nil || !t.observation.metricsEnabled {
		return
	}
	t.mu.Lock()
	if o := t.observation; !o.streams.Terminal && o.streams.Capacity != uint64(capacity) {
		o.streams.Capacity = uint64(capacity)
		o.streams.Version++
	}
	t.mu.Unlock()
}

func (t *http2Client) publishStreamObservation() {
	t.publishStreamSnapshot(false)
}

// The ticker catches stream orphan cleanup performed under controlBuf.mu, but
// need not repeat unchanged snapshots on every tick. A callback panic is an
// observation loss; retrying it could repeat partial SDK side effects. Embedded
// lifecycle snapshots do not advance this watermark: a recorder may discard a
// repeated lifecycle version without consuming its newer stream version.
func (t *http2Client) publishStreamObservationIfChanged() {
	t.publishStreamSnapshot(true)
}

func (t *http2Client) publishStreamSnapshot(changedOnly bool) {
	if t.observation == nil || !t.observation.metricsEnabled {
		return
	}
	t.mu.Lock()
	o := t.observation
	// Terminal lifecycle delivery carries the final embedded stream snapshot,
	// after all socket and stream cleanup. Do not interrupt that cleanup batch.
	if o.streams.Terminal || (changedOnly && o.streams.Version <= o.streamAttempted) {
		t.mu.Unlock()
		return
	}
	metrics, ok := o.recorder.(connstats.MetricsRecorder)
	event := o.streams
	if ok && event.Version > o.streamAttempted {
		o.streamAttempted = event.Version
	}
	t.mu.Unlock()
	if ok {
		safeObserveStreams(metrics, event)
	}
}

func safeObserveStreams(recorder connstats.MetricsRecorder, event connstats.StreamEvent) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			warnConnectionObserverPanic("ObserveStreams")
		}
	}()
	recorder.ObserveStreams(event)
	returned = true
}

// Error methods are application code too. Classify only after closing resources
// and outside transport locks, without letting a panic change transport cleanup.
func safeConnectionCloseReason(err error) (reason connstats.Reason) {
	reason = connstats.ReasonOther
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			reason = connstats.ReasonOther
			warnConnectionObserverPanic("CloseReason")
		}
	}()
	reason = connectionCloseReason(err)
	returned = true
	return reason
}

func connectionCloseReason(err error) connstats.Reason {
	if err == nil || errors.Is(err, ErrConnClosing) {
		return connstats.ReasonLocal
	}
	if connectionErr, ok := err.(ConnectionError); ok {
		if connectionErr.isIgnorable {
			return connstats.ReasonLocal
		}
		if connectionErr.err != nil {
			err = connectionErr.err
		} else {
			return connstats.ReasonProtocol
		}
	}
	if errors.Is(err, context.Canceled) {
		return connstats.ReasonCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return connstats.ReasonDeadline
	}
	var statusError *status.Error
	if errors.As(err, &statusError) {
		switch statusError.GRPCStatus().Code() {
		case codes.Canceled:
			return connstats.ReasonCanceled
		case codes.DeadlineExceeded:
			return connstats.ReasonDeadline
		}
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		if networkError.Timeout() {
			return connstats.ReasonDeadline
		}
		return connstats.ReasonNetwork
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
		return connstats.ReasonNetwork
	}
	return connstats.ReasonOther
}

func safeNewConnection(factory connstats.Observer, meta connstats.Meta) (r connstats.Recorder) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			r = nil
			warnConnectionObserverPanic("NewConnection")
		}
	}()
	r = factory.NewConnection(meta)
	returned = true
	return r
}

func safeObserveConnection(recorder connstats.Recorder, event connstats.Event) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			warnConnectionObserverPanic("Observe")
		}
	}()
	recorder.Observe(event)
	returned = true
}

// A broken observer must not panic into transport cleanup or flood the logs.
// The limit is shared across observer calls and transports, once per minute.
var connectionObserverLastWarning atomic.Int64

func warnConnectionObserverPanic(operation string) {
	// The application logger is another extension. Diagnostics must not
	// interrupt resource cleanup or invoke methods on arbitrary panic values.
	defer func() { _ = recover() }()
	now := time.Now().UnixNano()
	previous := connectionObserverLastWarning.Load()
	if previous != 0 && now-previous < int64(time.Minute) {
		return
	}
	if connectionObserverLastWarning.CompareAndSwap(previous, now) {
		klog.Warnf("KITEX: panic in gRPC connection observer %s; observation may be incomplete (warnings limited to once per minute)", operation)
	}
}
