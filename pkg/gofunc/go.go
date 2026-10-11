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

package gofunc

import (
	"context"
	"runtime/debug"
	"sync"

	"github.com/bytedance/gopkg/util/gopool"

	"github.com/cloudwego/kitex/internal/taskscope"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/profiler"
	"github.com/cloudwego/kitex/pkg/taskstats"
)

// GoTask is used to spawn a new task.
type GoTask func(context.Context, func())

// GoFunc is the default func used globally.
var GoFunc GoTask

func init() {
	GoFunc = func(ctx context.Context, f func()) {
		if holder := defaultTaskObserver.Load(); holder != nil && !observationOwned(ctx) {
			task := taskstats.Begin(taskscope.Observer(ctx, holder.observer), taskstats.Meta{
				Executor: "kitex.gofunc", Type: "pooled", Role: "global",
			})
			original := f
			f = func() { task.Run(original) }
		}
		gopool.CtxGo(ctx, func() {
			profiler.Tag(ctx)
			f()
			profiler.Untag(ctx)
		})
	}
}

// RecoverGoFuncWithInfo is the go func with recover panic and service info.
// It is used for panic defence and output key info for troubleshooting.
func RecoverGoFuncWithInfo(ctx context.Context, task func(), info *Info) {
	var observed *taskstats.Task
	var owner *observedContext
	if holder := defaultTaskObserver.Load(); holder != nil {
		// GoFunc is publicly replaceable. This helper does not assume that
		// its actual dispatcher is the shared gopool.
		observed = taskstats.Begin(taskscope.Observer(ctx, holder.observer), taskstats.Meta{
			Executor: "kitex.gofunc.recover", Type: "custom", Role: "global",
		})
		owner = &observedContext{Context: ctx}
		ctx = owner
	}
	submitted := false
	defer func() {
		if !submitted {
			if owner != nil {
				owner.retired.Store(true)
			}
			observed.Finish(taskstats.OutcomeError)
		}
	}()
	GoFunc(ctx, func() {
		if owner != nil {
			owner.retired.Store(true)
		}
		observed.Start()
		outcome := taskstats.OutcomeAborted
		defer func() {
			observed.Finish(outcome)
			infoPool.Put(info)
		}()
		returned := false
		var panicErr interface{}
		var panicStack string
		func() {
			defer func() {
				panicErr = recover()
				if panicErr != nil {
					panicStack = string(debug.Stack())
				}
			}()
			task()
			returned = true
		}()
		if returned {
			outcome = taskstats.OutcomeSuccess
			return
		}
		// Recovering panic(nil) also returns here on Go 1.20. Goexit does
		// not return from the inner function and is reported as aborted.
		outcome = taskstats.OutcomePanic
		if panicErr != nil {
			if info.RemoteService == "" {
				info.RemoteService = "unknown"
			}
			if info.RemoteAddr == "" {
				info.RemoteAddr = "unknown"
			}
			stack := panicStack
			klog.CtxErrorf(ctx, "KITEX: panic happened, remoteService=%s remoteAddress=%s error=%v\nstack=%s",
				info.RemoteService, info.RemoteAddr, panicErr, stack)
			phLock.RLock()
			if panicHandler != nil {
				panicHandler(info, panicErr, stack)
			}
			phLock.RUnlock()
		}
	})
	submitted = true
}

// SetPanicHandler is used to do something when panic happen, for example do metric report.
func SetPanicHandler(hdlr func(info *Info, panicErr interface{}, panicStack string)) {
	phLock.Lock()
	panicHandler = hdlr
	phLock.Unlock()
}

// NewBasicInfo is to new Info with remoteService  and remoteAddr.
func NewBasicInfo(remoteService, remoteAddr string) *Info {
	info := infoPool.Get().(*Info)
	info.RemoteService = remoteService
	info.RemoteAddr = remoteAddr
	return info
}

// Info is used to pass key info to go func, which is convenient for troubleshooting
type Info struct {
	RemoteService string
	RemoteAddr    string
}

var (
	// EmptyInfo is empty Info which is convenient to reuse
	EmptyInfo = &Info{}

	panicHandler func(info *Info, panicErr interface{}, panicStack string)
	phLock       sync.RWMutex

	infoPool = &sync.Pool{
		New: func() interface{} {
			return new(Info)
		},
	}
)
