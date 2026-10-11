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

package client

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/profiler"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/taskstats"
)

// timeoutPool is a worker pool for task with timeout
type timeoutPool struct {
	size  int32
	tasks chan *timeoutTask

	// maxIdle is the number of the max idle workers in the pool.
	// if maxIdle too small, the pool works like a native 'go func()'.
	maxIdle int32
	// maxIdleTime is the max idle time that the worker will wait for the new task.
	maxIdleTime time.Duration

	mu     sync.Mutex
	ticker chan struct{}
}

// newTimeoutPool ...
func newTimeoutPool(maxIdle int, maxIdleTime time.Duration) *timeoutPool {
	return &timeoutPool{
		tasks:       make(chan *timeoutTask),
		maxIdle:     int32(maxIdle),
		maxIdleTime: maxIdleTime,
	}
}

// Size returns the number of the running workers.
func (p *timeoutPool) Size() int32 {
	return atomic.LoadInt32(&p.size)
}

func (p *timeoutPool) createTicker() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// make sure previous goroutine will be closed before creating a new one
	if p.ticker != nil {
		close(p.ticker)
	}
	ch := make(chan struct{})
	p.ticker = ch

	go func(done <-chan struct{}) {
		// if maxIdleTime=60s, maxIdle=100
		// it sends noop task every 60ms
		// but always d >= 10*time.Millisecond
		// this may cause goroutines take more time to exit which is acceptable.
		d := p.maxIdleTime / time.Duration(p.maxIdle) / 10
		if d < 10*time.Millisecond {
			d = 10 * time.Millisecond
		}
		tk := time.NewTicker(d)
		defer tk.Stop()
		for p.Size() > 0 {
			select {
			case <-tk.C:
			case <-done:
				return
			}
			select {
			case p.tasks <- nil: // noop task for checking idletime
			case <-tk.C:
			}
		}
	}(ch)
}

func (p *timeoutPool) createWorker(t *timeoutTask) bool {
	if n := atomic.AddInt32(&p.size, 1); n < p.maxIdle {
		if n == 1 {
			p.createTicker()
		}
		go func(t *timeoutTask) {
			defer atomic.AddInt32(&p.size, -1)

			t.Run()

			lastactive := time.Now()
			for t := range p.tasks {
				if t == nil { // from `createTicker` func
					if time.Since(lastactive) > p.maxIdleTime {
						break
					}
					continue
				}
				t.Run()
				lastactive = time.Now()
			}
		}(t)
		return true
	} else {
		atomic.AddInt32(&p.size, -1)
		return false
	}
}

// RunTask creates/reuses a worker to run task.
//
// It returns:
// - the underlying ctx used for calling endpoint.Endpoint
// - err returned by endpoint.Endpoint or ctx.Err()
//
// NOTE:
// Err() method of the ctx will always be set context.Canceled after the func returns,
// because ctx.Done() will be closed, and context package requires Err() must not nil.
// Caller should always check the err first,
// if it's nil, which means everything is ok, no need to do the further ctx.Err() check.
func (p *timeoutPool) RunTask(ctx context.Context, timeout time.Duration,
	req, resp any, ep endpoint.Endpoint,
) (context.Context, error) {
	return p.runTask(ctx, timeout, req, resp, ep, nil, "")
}

func (p *timeoutPool) runTask(ctx context.Context, timeout time.Duration, req, resp any,
	ep endpoint.Endpoint, observer taskstats.Observer, service string,
) (context.Context, error) {
	t := newTimeoutTask(ctx, timeout, req, resp, ep)
	if observer != nil {
		t.observationReady = make(chan struct{})
		t.observationSubmitted = time.Now()
	}
	activate := func(overflow bool) {
		if observer == nil {
			return
		}
		meta := taskstats.Meta{Executor: "kitex.timeout.reuse", Type: "reusable", Role: "client", Service: service}
		if overflow {
			meta.Executor, meta.Type = "kitex.timeout.overflow", "native"
		}
		t.observation = taskstats.BeginAt(observer, meta, t.observationSubmitted)
		close(t.observationReady)
	}
	select {
	case p.tasks <- t:
		activate(false)
		return t.Wait()
	default:
	}
	if !p.createWorker(t) {
		activate(true)
		// if created worker, t.Run() will be called in worker goroutine
		// if NOT, we should go t.Run() here.
		go t.Run()
	} else {
		activate(false)
	}
	return t.Wait()
}

var poolTask = sync.Pool{
	New: func() any {
		return &timeoutTask{}
	},
}

// timeoutTask is the function that the worker will execute.
type timeoutTask struct {
	ctx *timeoutContext

	wg sync.WaitGroup

	req, resp any
	ep        endpoint.Endpoint

	err                  atomic.Value
	observation          *taskstats.Task
	observationReady     chan struct{}
	observationSubmitted time.Time
}

func newTimeoutTask(ctx context.Context, timeout time.Duration,
	req, resp any, ep endpoint.Endpoint,
) *timeoutTask {
	t := poolTask.Get().(*timeoutTask)

	// timeoutContext must not be reused,
	// coz user may keep ref to it even though after calling endpoint.Endpoint
	t.ctx = newTimeoutContext(ctx, timeout)

	t.req, t.resp = req, resp
	t.ep = ep
	t.err = atomic.Value{}
	t.wg.Add(1) // for Wait, Wait must be called before Recycle()
	return t
}

func (t *timeoutTask) recycle() {
	// make sure Wait done before returning it to pool
	t.wg.Wait()

	t.ctx = nil
	t.req, t.resp = nil, nil
	t.ep = nil
	t.err = atomic.Value{}
	t.observation = nil
	t.observationReady = nil
	t.observationSubmitted = time.Time{}
	poolTask.Put(t)
}

func (t *timeoutTask) Cancel(err error) {
	t.ctx.Cancel(err)
}

// Run must be called in a separated goroutine
func (t *timeoutTask) Run() {
	if t.observationReady != nil {
		<-t.observationReady
	}
	t.observation.Start()
	outcome := taskstats.OutcomeAborted
	defer func() {
		var finished time.Time
		if t.observation != nil {
			finished = time.Now()
		}
		t.Cancel(context.Canceled)
		if t.observation != nil {
			// Caller classification precedes the terminal snapshot, while the
			// actual endpoint exit timestamp is retained.
			t.wg.Wait()
			t.observation.FinishAt(outcome, finished)
		}
		t.recycle()
	}()
	func() {
		defer func() {
			if panicInfo := recover(); panicInfo != nil {
				outcome = taskstats.OutcomePanic
				ri := rpcinfo.GetRPCInfo(t.ctx)
				if ri != nil {
					t.err.Store(rpcinfo.ClientPanicToErr(t.ctx, panicInfo, ri, true))
				} else {
					t.err.Store(fmt.Errorf("KITEX: panic without rpcinfo, error=%v\nstack=%s",
						panicInfo, debug.Stack()))
				}
			}
		}()
		var err error
		if profiler.IsEnabled(t.ctx) {
			profiler.Tag(t.ctx)
			err = t.ep(t.ctx, t.req, t.resp)
			profiler.Untag(t.ctx)
		} else {
			err = t.ep(t.ctx, t.req, t.resp)
		}
		outcome = taskstats.OutcomeSuccess
		if err != nil { // panic if store nil
			outcome = taskstats.OutcomeError
			t.err.Store(err)
		}
	}()
	if outcome == taskstats.OutcomeAborted {
		// A recovered legacy panic(nil) returns from the inner function;
		// runtime.Goexit continues defers without reaching this statement.
		outcome = taskstats.OutcomePanic
	}
}

// Wait waits Run finishes and returns result
func (t *timeoutTask) Wait() (returnedCtx context.Context, err error) {
	var callerCause error
	defer func() {
		t.observation.CallerReturned(callerCause == context.DeadlineExceeded, callerCause == context.Canceled)
		t.wg.Done()
	}()
	dl, ok := t.ctx.Deadline()
	if !ok {
		return t.waitNoTimeoutObserved(&callerCause)
	}
	d := time.Until(dl)
	if d < 0 {
		callerCause = context.DeadlineExceeded
		t.Cancel(context.DeadlineExceeded)
		return t.ctx, t.ctx.Err()
	}
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-t.ctx.Done(): // finished before timeout
		v := t.err.Load()
		if v != nil {
			return t.ctx, v.(error)
		}
		return t.ctx, nil

	case <-t.ctx.Context.Done(): // parent done
		callerCause = t.ctx.Context.Err()
		t.Cancel(callerCause)
	case <-tm.C: // timeout
		callerCause = context.DeadlineExceeded
		t.Cancel(callerCause)
	}
	return t.ctx, t.ctx.Err()
}

func (t *timeoutTask) waitNoTimeoutObserved(callerCause *error) (context.Context, error) {
	select {
	case <-t.ctx.Done(): // finished before parent done
		v := t.err.Load()
		if v != nil {
			return t.ctx, v.(error)
		}
		return t.ctx, nil

	case <-t.ctx.Context.Done(): // parent done
		*callerCause = t.ctx.Context.Err()
		t.Cancel(*callerCause)
		return t.ctx, t.ctx.Err()
	}
}

type timeoutContext struct {
	context.Context

	dl time.Time
	ch chan struct{}

	mu  sync.Mutex
	err error
}

func newTimeoutContext(ctx context.Context, timeout time.Duration) *timeoutContext {
	ret := &timeoutContext{Context: ctx, ch: make(chan struct{})}
	deadline, ok := ctx.Deadline()
	if ok {
		ret.dl = deadline
	}
	if timeout != 0 {
		dl := time.Now().Add(timeout)
		if ret.dl.IsZero() || dl.Before(ret.dl) {
			// The new deadline is sooner than the ctx one
			ret.dl = dl
		}
	}
	return ret
}

func (p *timeoutContext) Deadline() (deadline time.Time, ok bool) {
	return p.dl, !p.dl.IsZero()
}

func (p *timeoutContext) Done() <-chan struct{} {
	return p.ch
}

func (p *timeoutContext) Err() error {
	p.mu.Lock()
	err := p.err
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.Context.Err()
}

func (p *timeoutContext) Cancel(err error) {
	p.mu.Lock()
	if err != nil && p.err == nil {
		p.err = err
		close(p.ch)
	}
	p.mu.Unlock()
}
