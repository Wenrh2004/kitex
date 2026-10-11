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

package gofunc

import (
	"context"
	"sync/atomic"

	"github.com/bytedance/gopkg/util/gopool"

	"github.com/cloudwego/kitex/internal/taskscope"
	"github.com/cloudwego/kitex/pkg/taskstats"
)

// observedContext carries one recover helper's handoff through standard derived
// contexts. The marker retires when the accepted closure starts, so independent
// child submissions from that closure can be observed normally.
type observedContext struct {
	context.Context
	retired atomic.Bool
}

type observationKey struct{}

func (ctx *observedContext) Value(key interface{}) interface{} {
	if _, ok := key.(observationKey); ok {
		return ctx
	}
	return ctx.Context.Value(key)
}

func observationOwned(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	owner, ok := ctx.Value(observationKey{}).(*observedContext)
	return ok && !owner.retired.Load()
}

type observerHolder struct{ observer taskstats.Observer }

var defaultTaskObserver atomic.Pointer[observerHolder]

// SetTaskObserver enables observation of submissions through the default GoFunc
// implementation and RecoverGoFuncWithInfo. Each task retains its original recorder through completion.
// It does not instrument Netpoll or other users of the shared global gopool.
// For a replacement dispatcher, SetTaskObserver still observes the recover helper;
// use WrapTask for its other submissions. Set nil to disable.
func SetTaskObserver(observer taskstats.Observer) {
	if observer == nil {
		defaultTaskObserver.Store(nil)
		return
	}
	defaultTaskObserver.Store(&observerHolder{observer: observer})
}

// WrapTask wraps an executor without changing its scheduling/panic semantics.
// Install a replacement GoFunc before concurrent use, as with the existing
// public GoFunc variable. next must execute each accepted task exactly once and
// must not report a submission failure after accepting work that may still
// execute. Panics from synchronous execution may propagate. Its API cannot
// report asynchronous rejection or distinguish failed submission from accepted
// work. A function that recovers its own
// panic returns successfully to this generic wrapper. Use SetTaskObserver to
// observe business panics caught inside RecoverGoFuncWithInfo; that helper's
// owned recorder prevents a second count when the dispatcher is also wrapped.
// A replacement dispatcher may derive the handoff context, but must pass it
// only along the same accepted function's dispatch chain until execution starts.
// An independent submission before that boundary needs its own context; GoTask
// does not expose function identity or asynchronous acceptance/rejection.
// A synchronous submission panic attempts to finish the observed task as an
// error and propagates to the caller; an existing terminal outcome is preserved.
// That outcome relies on next's acceptance contract. It does not detect silent
// rejection or cancel work already accepted.
func WrapTask(next GoTask, observer taskstats.Observer, meta taskstats.Meta) GoTask {
	if observer == nil {
		return next
	}
	return func(ctx context.Context, f func()) {
		if observationOwned(ctx) {
			next(ctx, f)
			return
		}
		task := taskstats.Begin(taskscope.Observer(ctx, observer), meta)
		returned := false
		defer func() {
			if !returned {
				task.Finish(taskstats.OutcomeError)
			}
		}()
		next(ctx, func() { task.Run(f) })
		returned = true
	}
}

// DefaultPoolSnapshot describes the actual shared global gopool, including other
// users such as Netpoll, even when GoFunc uses a replacement dispatcher. The
// dependency exposes only its reported WorkerCount, not physical active/idle workers
// or queue length. Its SetCap is a soft scaling threshold, and its linked-list
// queue has no hard bound; neither provides a hard capacity to report. All other
// capabilities remain unknown. Never label this snapshot by service or derive
// physical values from the separately observed Kitex tasks.
// In gopool v0.1.4, a task calling runtime.Goexit can leave WorkerCount stale;
// this public counter must not be treated as an exact live goroutine census.
func DefaultPoolSnapshot() taskstats.PoolSnapshot {
	return taskstats.PoolSnapshot{Workers: int64(gopool.WorkerCount()), WorkersKnown: true}
}
