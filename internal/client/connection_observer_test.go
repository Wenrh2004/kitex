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

package client

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/cloudwego/kitex/pkg/connpool"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/connstats"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/stats"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/cloudwego/kitex/transport"
)

type configuredConnectionObserver struct {
	mu     sync.Mutex
	meta   []connstats.Meta
	events []connstats.Event
}

type configuredConnectionRecorder struct {
	observer *configuredConnectionObserver
	index    int
}

func (o *configuredConnectionObserver) NewConnection(meta connstats.Meta) connstats.Recorder {
	o.mu.Lock()
	index := len(o.meta)
	o.meta = append(o.meta, meta)
	o.events = append(o.events, connstats.Event{})
	o.mu.Unlock()
	return &configuredConnectionRecorder{observer: o, index: index}
}

func (r *configuredConnectionRecorder) Observe(event connstats.Event) {
	o := r.observer
	o.mu.Lock()
	if event.Version > o.events[r.index].Version {
		o.events[r.index] = event
	}
	o.mu.Unlock()
}

func (o *configuredConnectionObserver) snapshot() ([]connstats.Meta, []connstats.Event) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]connstats.Meta(nil), o.meta...), append([]connstats.Event(nil), o.events...)
}

func awaitConfiguredConnectionState(t *testing.T, observer *configuredConnectionObserver, want connstats.State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, events := observer.snapshot()
		if len(events) == 1 && events[0].State == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	_, events := observer.snapshot()
	t.Fatalf("observed events = %v, want one connection in state %v", events, want)
}

// This peer only exchanges the HTTP/2 preface and SETTINGS. An in-memory pipe
// exercises actual connection creation without a listener or external server.
func connectionOptionDialer(t *testing.T) (remote.Dialer, func(), <-chan struct{}) {
	t.Helper()
	var servers []net.Conn
	header := make(chan struct{}, 1)
	closeServers := func() {
		for _, server := range servers {
			_ = server.Close()
		}
	}
	t.Cleanup(closeServers)
	dialer := &remote.SynthesizedDialer{DialFunc: func(string, string, time.Duration) (net.Conn, error) {
		client, server := net.Pipe()
		servers = append(servers, server)
		t.Cleanup(func() { _ = client.Close() })
		go func() {
			preface := make([]byte, len(http2.ClientPreface))
			if _, err := io.ReadFull(server, preface); err != nil {
				return
			}
			framer := http2.NewFramer(server, server)
			if _, err := framer.ReadFrame(); err != nil {
				return
			}
			if err := framer.WriteSettings(); err != nil {
				return
			}
			for {
				frame, err := framer.ReadFrame()
				if err != nil {
					return
				}
				if _, ok := frame.(*http2.HeadersFrame); ok {
					header <- struct{}{}
				}
			}
		}()
		return client, nil
	}}
	return dialer, closeServers, header
}

func TestConnectionObserverRemoteOptions(t *testing.T) {
	for _, protocol := range []transport.Protocol{transport.GRPC, transport.GRPCStreaming} {
		for _, short := range []bool{false, true} {
			name := protocol.String() + "/long"
			if short {
				name = protocol.String() + "/short"
			}
			t.Run(name, func(t *testing.T) {
				observer := &configuredConnectionObserver{}
				opts := NewOptions([]Option{{F: func(o *Options, _ *utils.Slice) {
					mutable := rpcinfo.AsMutableRPCConfig(o.Configs)
					mutable.SetTransportProtocol(protocol)
					o.Svr.ServiceName = "configured-service"
					o.GRPCConnPoolSize = 1
					o.GRPCConnectOpts.ConnectionObserver = observer
					if short {
						o.PoolCfg = &connpool.IdleConfig{}
					}
				}}})
				pool := opts.RemoteOpt.ConnPool
				if protocol == transport.GRPCStreaming {
					pool = opts.RemoteOpt.GRPCStreamingConnPool
				}
				if pool == nil {
					t.Fatal("gRPC connection pool was not configured")
				}
				t.Cleanup(func() { _ = opts.RemoteOpt.ConnPool.Close() })
				t.Cleanup(func() { _ = pool.Close() })
				if opts.StatsLevel == nil || *opts.StatsLevel != stats.LevelDisabled || opts.TracerCtl.HasTracer() {
					t.Fatal("fixture must have RPC tracing and RPC stats disabled")
				}
				cfg := rpcinfo.NewRPCConfig()
				rpcinfo.AsMutableRPCConfig(cfg).SetPayloadCodec(serviceinfo.Protobuf)
				invocation := rpcinfo.NewInvocation("configured-service", "method")
				invocation.SetStreamingMode(serviceinfo.StreamingBidirectional)
				ri := rpcinfo.NewRPCInfo(
					rpcinfo.NewEndpointInfo("caller", "method", nil, nil),
					rpcinfo.NewEndpointInfo("configured-service", "method", nil, nil),
					invocation, cfg, rpcinfo.NewRPCStats())
				ctx, cancel := context.WithTimeout(rpcinfo.NewCtxWithRPCInfo(context.Background(), ri), 2*time.Second)
				defer cancel()
				dialer, closeServers, header := connectionOptionDialer(t)
				conn, err := pool.Get(ctx, "tcp", "configured-target:1234", remote.ConnOption{Dialer: dialer, ConnectTimeout: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-header:
				case <-time.After(2 * time.Second):
					t.Fatal("stream header was not written before draining")
				}
				awaitConfiguredConnectionState(t, observer, connstats.StateReachable)
				metas, _ := observer.snapshot()
				wantMode := connstats.ModeLong
				if short {
					wantMode = connstats.ModeShort
				}
				if metas[0].Service != "configured-service" || metas[0].Target != "configured-target:1234" || metas[0].Mode != wantMode || metas[0].PoolID == 0 {
					t.Fatalf("ConnectOptions were not preserved through %s: %+v", name, metas[0])
				}
				if short {
					if err := pool.Put(conn); err != nil {
						t.Fatal(err)
					}
				} else if err := pool.Close(); err != nil {
					t.Fatal(err)
				}
				awaitConfiguredConnectionState(t, observer, connstats.StateDraining)
				closeServers()
				awaitConfiguredConnectionState(t, observer, connstats.StateClosed)
			})
		}
	}
}
