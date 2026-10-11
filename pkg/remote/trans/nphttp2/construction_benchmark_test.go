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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/gofunc"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/taskstats"
)

type (
	constructionBenchmarkObserver struct{}
	constructionBenchmarkRecorder struct{}
)

func (constructionBenchmarkObserver) NewTask(taskstats.Meta) taskstats.Recorder {
	return constructionBenchmarkRecorder{}
}
func (constructionBenchmarkRecorder) Observe(taskstats.Event) {}

// ShortConn constructs on every request. This fixture uses the same bounded
// dispatcher and mock netpoll connection on both revisions, and joins actual
// writer execution on every iteration. It measures construction/stream/cleanup
// cost, not network latency or the default gopool scheduler.
func BenchmarkConnectionConstruction(b *testing.B) {
	oldDispatch := gofunc.GoFunc
	defer func() { gofunc.GoFunc = oldDispatch; gofunc.SetTaskObserver(nil) }()
	var workers sync.WaitGroup
	gofunc.GoFunc = func(_ context.Context, f func()) {
		workers.Add(1)
		go func() { defer workers.Done(); f() }()
	}
	for _, path := range []string{"short", "preface-failure"} {
		b.Run(path, func(b *testing.B) {
			for _, enabled := range []bool{false, true} {
				name := "disabled"
				if enabled {
					name = "enabled"
				}
				b.Run(name, func(b *testing.B) {
					gofunc.SetTaskObserver(nil)
					if enabled {
						gofunc.SetTaskObserver(constructionBenchmarkObserver{})
					}
					opts := slotTestOpts
					opts.ShortConn = true
					pool := NewConnPool("test", 1, opts)
					b.Cleanup(func() { _ = pool.Close() })
					failure := errors.New("benchmark preface failure")
					dialer := &remote.SynthesizedDialer{DialFunc: func(string, string, time.Duration) (net.Conn, error) {
						conn := newMockNpConn(mockAddr0)
						if path == "preface-failure" {
							conn.WriteFunc = func([]byte) (int, error) { return 0, failure }
						}
						return conn, nil
					}}
					ctx := newMockCtxWithRPCInfo(serviceinfo.StreamingBidirectional)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						conn, err := pool.Get(ctx, "tcp", mockAddr0, remote.ConnOption{Dialer: dialer, ConnectTimeout: time.Second})
						if path == "preface-failure" {
							if err == nil || conn != nil {
								b.Fatal("expected preface construction failure")
							}
						} else {
							if err != nil {
								b.Fatal(err)
							}
							cc := conn.(*clientConn)
							cc.tr.CloseStream(cc.s, nil)
							if err := pool.Put(conn); err != nil {
								b.Fatal(err)
							}
						}
						workers.Wait()
					}
				})
			}
		})
	}
}
