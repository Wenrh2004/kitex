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

// Package taskscope defers observation during transport construction without
// deferring execution. It is an internal boundary, not an admission mechanism.
package taskscope

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/taskstats"
)

const maxDeferred = 16

type contextKey struct{}

// Scope retains at most sixteen deferred recorders, each with one absolute
// snapshot. Overflow disables observation of that submission, never execution.
type Scope struct {
	parent   context.Context
	mu       sync.Mutex
	active   bool
	records  *[maxDeferred]*recorder
	count    int
	overflow bool
}

// NewContext marks a Kitex-owned construction. Activate must be called by its
// owner after leaving transport locks and singleflight, including on failure.
// As with standard derived contexts, ctx must not be nil.
func NewContext(ctx context.Context) (context.Context, *Scope) {
	if ctx == nil {
		panic("cannot create context from nil parent")
	}
	s := &Scope{parent: ctx}
	return s, s
}

func (s *Scope) Deadline() (time.Time, bool) { return s.parent.Deadline() }

func (s *Scope) Done() <-chan struct{} { return s.parent.Done() }

func (s *Scope) Err() error { return s.parent.Err() }

func (s *Scope) Value(key interface{}) interface{} {
	if _, ok := key.(contextKey); ok {
		return s
	}
	return s.parent.Value(key)
}

// Observer captures the submission's observer. It leaves disabled observation
// untouched and never re-reads the global observer at activation.
func Observer(ctx context.Context, observer taskstats.Observer) taskstats.Observer {
	if observer == nil {
		return nil
	}
	if ctx == nil {
		return observer
	}
	if s, ok := ctx.Value(contextKey{}).(*Scope); ok {
		return scopedObserver{scope: s, observer: observer}
	}
	return observer
}

type scopedObserver struct {
	scope    *Scope
	observer taskstats.Observer
}

func (o scopedObserver) NewTask(meta taskstats.Meta) taskstats.Recorder {
	s := o.scope
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		return o.observer.NewTask(meta)
	}
	if s.count == maxDeferred {
		s.overflow = true
		s.mu.Unlock()
		return nil
	}
	r := &recorder{observer: o.observer, meta: meta}
	if s.records == nil {
		s.records = new([maxDeferred]*recorder)
	}
	s.records[s.count] = r
	s.count++
	s.mu.Unlock()
	return r
}

// Activate is claimed once without waiting for callbacks owned by another
// caller. Factories and delivery run outside all scope and recorder locks.
func (s *Scope) Activate() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		return
	}
	s.active = true
	records, count, overflow := s.records, s.count, s.overflow
	s.records = nil
	s.count = 0
	s.mu.Unlock()
	if overflow {
		warn("construction capacity exceeded")
	}
	if records != nil {
		for _, r := range records[:count] {
			r.activate()
		}
	}
}

type recorder struct {
	mu       sync.Mutex
	observer taskstats.Observer
	meta     taskstats.Meta
	latest   taskstats.Event
	live     taskstats.Recorder
	disabled bool
}

func (r *recorder) Observe(event taskstats.Event) {
	r.mu.Lock()
	if r.disabled || event.Version <= r.latest.Version || r.latest.State == taskstats.StateFinished {
		r.mu.Unlock()
		return
	}
	r.latest = event
	live := r.live
	r.mu.Unlock()
	if live != nil {
		publish(live, event)
	}
}

func (r *recorder) activate() {
	// Only Scope's unique activation owner reaches this method. While the
	// factory runs, actual tasks continue updating their latest snapshot.
	live := newTask(r.observer, r.meta)
	r.mu.Lock()
	r.observer = nil
	r.meta = taskstats.Meta{}
	if live == nil {
		r.disabled = true
		r.latest = taskstats.Event{}
		r.mu.Unlock()
		return
	}
	r.live = live
	event := r.latest
	r.mu.Unlock()
	if event.Version != 0 {
		publish(live, event)
	}
}

func newTask(observer taskstats.Observer, meta taskstats.Meta) (live taskstats.Recorder) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			live = nil
			warn("NewTask panic")
		}
	}()
	live = observer.NewTask(meta)
	returned = true
	return live
}

func publish(live taskstats.Recorder, event taskstats.Event) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			warn("Observe panic")
		}
	}()
	live.Observe(event)
	returned = true
}

var lastWarning atomic.Int64

func warn(phase string) {
	defer func() { _ = recover() }()
	now := time.Now().UnixNano()
	previous := lastWarning.Load()
	if previous != 0 && now-previous < int64(time.Minute) {
		return
	}
	if lastWarning.CompareAndSwap(previous, now) {
		klog.Warnf("KITEX: task construction observation %s; observation may be incomplete", phase)
	}
}
