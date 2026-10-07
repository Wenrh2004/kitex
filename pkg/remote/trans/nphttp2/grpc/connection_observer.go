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
	metricsEnabled    bool
	streams           connstats.StreamEvent
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
	event := o.latest
	event.Streams = o.streams
	t.mu.Unlock()
	if r != nil {
		safeObserveConnection(r, event)
	}
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
	if t.observation == nil || !t.observation.metricsEnabled {
		return
	}
	t.mu.Lock()
	r, event := t.observation.recorder, t.observation.streams
	t.mu.Unlock()
	// Terminal lifecycle delivery carries the final embedded stream snapshot,
	// after all socket and stream cleanup. Do not interrupt that cleanup batch.
	if event.Terminal {
		return
	}
	if metrics, ok := r.(connstats.MetricsRecorder); ok {
		safeObserveStreams(metrics, event)
	}
}

func safeObserveStreams(recorder connstats.MetricsRecorder, event connstats.StreamEvent) {
	defer func() {
		if p := recover(); p != nil {
			warnConnectionObserverPanic("ObserveStreams", p)
		}
	}()
	recorder.ObserveStreams(event)
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
	defer func() {
		if p := recover(); p != nil {
			r = nil
			warnConnectionObserverPanic("NewConnection", p)
		}
	}()
	return factory.NewConnection(meta)
}

func safeObserveConnection(recorder connstats.Recorder, event connstats.Event) {
	defer func() {
		if p := recover(); p != nil {
			warnConnectionObserverPanic("Observe", p)
		}
	}()
	recorder.Observe(event)
}

// A broken observer must not panic into transport cleanup or flood the logs.
// The limit is shared across observer calls and transports, once per minute.
var connectionObserverLastWarning atomic.Int64

func warnConnectionObserverPanic(operation string, recovered interface{}) {
	now := time.Now().UnixNano()
	previous := connectionObserverLastWarning.Load()
	if previous != 0 && now-previous < int64(time.Minute) {
		return
	}
	if connectionObserverLastWarning.CompareAndSwap(previous, now) {
		klog.Warnf("KITEX: gRPC connection observer %s panic (warnings limited to once per minute): %v", operation, recovered)
	}
}
