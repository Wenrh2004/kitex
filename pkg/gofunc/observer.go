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

	"github.com/cloudwego/kitex/pkg/taskstats"
)

// observedContext marks a helper that already owns its task recorder. It is
// created only when observation is enabled and prevents duplicate counting by
// the default GoFunc implementation.
type observedContext struct{ context.Context }

func observationOwned(ctx context.Context) bool { _, ok := ctx.(*observedContext); return ok }

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
// public GoFunc variable. next must execute each accepted task exactly once;
// its API cannot report asynchronous rejection. A function that recovers its own
// panic returns successfully to this generic wrapper. Use SetTaskObserver to
// observe business panics caught inside RecoverGoFuncWithInfo; that helper's
// owned recorder prevents a second count when the dispatcher is also wrapped.
// A synchronous submission panic
// finishes a still-pending task as an error and is propagated to the caller.
func WrapTask(next GoTask, observer taskstats.Observer, meta taskstats.Meta) GoTask {
	if observer == nil {
		return next
	}
	return func(ctx context.Context, f func()) {
		if observationOwned(ctx) {
			next(ctx, f)
			return
		}
		task := taskstats.Begin(observer, meta)
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
// users such as Netpoll. Only WorkerCount is exposed by the dependency safely;
// queue/configuration values remain unknown. Never label this snapshot by service.
func DefaultPoolSnapshot() taskstats.PoolSnapshot {
	return taskstats.PoolSnapshot{Workers: int64(gopool.WorkerCount()), WorkersKnown: true}
}
