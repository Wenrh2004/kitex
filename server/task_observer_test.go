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

package server

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	internalserver "github.com/cloudwego/kitex/internal/server"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/taskstats"
	"github.com/cloudwego/kitex/pkg/utils"
)

type handlerObserver struct {
	metas  []taskstats.Meta
	events []taskstats.Event
}

func (o *handlerObserver) NewTask(m taskstats.Meta) taskstats.Recorder {
	o.metas = append(o.metas, m)
	return o
}
func (o *handlerObserver) Observe(e taskstats.Event) { o.events = append(o.events, e) }

func TestWithTaskObserverHandlerLifecycle(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, want := range []taskstats.Outcome{taskstats.OutcomeSuccess, taskstats.OutcomeError, taskstats.OutcomePanic} {
			o := &handlerObserver{}
			opts := internalserver.NewOptions(nil)
			var debug utils.Slice
			WithTaskObserver(o).F(opts, &debug)
			if opts.TaskObserver != o {
				t.Fatal("observer not applied")
			}
			method := serviceinfo.NewMethodInfo(func(context.Context, interface{}, interface{}, interface{}) error {
				switch want {
				case taskstats.OutcomePanic:
					panic("biz")
				case taskstats.OutcomeError:
					return errors.New("biz")
				default:
					return nil
				}
			}, func() interface{} { return nil }, func() interface{} { return nil }, false)
			svcInfo := &serviceinfo.ServiceInfo{ServiceName: "service", Methods: map[string]serviceinfo.MethodInfo{"method": method}}
			svcs := newServices()
			svcs.addService(svcInfo, nil, &RegisterOptions{})
			s := &server{opt: opts, svcs: svcs}
			inv := rpcinfo.NewInvocation("service", "method")
			inv.SetMethodInfo(method)
			ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), rpcinfo.NewRPCInfo(nil, nil, inv, rpcinfo.NewRPCConfig(), rpcinfo.NewRPCStats()))
			var err error
			if stream {
				err = s.streamHandleEndpoint()(ctx, nil)
			} else {
				err = s.invokeHandleEndpoint()(ctx, nil, nil)
			}
			if (err == nil) != (want == taskstats.OutcomeSuccess) {
				t.Fatalf("error %v", err)
			}
			if len(o.events) != 3 || len(o.metas) != 1 || o.events[2].Outcome != want || o.metas[0].Service != "service" || o.metas[0].Type != "inline" {
				t.Fatalf("%+v %+v", o.metas, o.events)
			}
			WithTaskObserver(nil).F(opts, &debug)
			if opts.TaskObserver != nil {
				t.Fatal("nil did not disable")
			}
		}
	}
}

func TestObservedServerGoexitAndPanicNil(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, tc := range []struct {
			name string
			f    func()
			want taskstats.Outcome
		}{{"goexit", runtime.Goexit, taskstats.OutcomeAborted}, {"panic_nil", func() { panic(nil) }, taskstats.OutcomePanic}} {
			o := &handlerObserver{}
			opts := internalserver.NewOptions(nil)
			opts.TaskObserver = o
			method := serviceinfo.NewMethodInfo(func(context.Context, interface{}, interface{}, interface{}) error { tc.f(); return nil }, func() interface{} { return nil }, func() interface{} { return nil }, false)
			info := &serviceinfo.ServiceInfo{ServiceName: "svc", Methods: map[string]serviceinfo.MethodInfo{"method": method}}
			svcs := newServices()
			svcs.addService(info, nil, &RegisterOptions{})
			s := &server{opt: opts, svcs: svcs}
			inv := rpcinfo.NewInvocation("svc", "method")
			inv.SetMethodInfo(method)
			ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), rpcinfo.NewRPCInfo(nil, nil, inv, rpcinfo.NewRPCConfig(), rpcinfo.NewRPCStats()))
			done := make(chan struct{})
			go func() {
				defer close(done)
				if stream {
					_ = s.streamHandleEndpoint()(ctx, nil)
				} else {
					_ = s.invokeHandleEndpoint()(ctx, nil, nil)
				}
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("handler stuck")
			}
			if len(o.events) != 3 || o.events[2].Outcome != tc.want {
				t.Fatalf("%s stream=%v: %+v", tc.name, stream, o.events)
			}
		}
	}
}
