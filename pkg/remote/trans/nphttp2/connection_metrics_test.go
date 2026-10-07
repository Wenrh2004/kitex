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
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/remote/connstats"
)

type poolDialObserver struct {
	poolConnectionObserver
	dialMu sync.Mutex
	dials  []connstats.DialEvent
	metas  []connstats.Meta
	hook   func()
}

func (o *poolDialObserver) ObserveDial(meta connstats.Meta, event connstats.DialEvent) {
	o.dialMu.Lock()
	o.dials = append(o.dials, event)
	o.metas = append(o.metas, meta)
	o.dialMu.Unlock()
	if o.hook != nil {
		o.hook()
	}
}

func (o *poolDialObserver) snapshot() ([]connstats.DialEvent, []connstats.Meta) {
	o.dialMu.Lock()
	defer o.dialMu.Unlock()
	return append([]connstats.DialEvent(nil), o.dials...), append([]connstats.Meta(nil), o.metas...)
}

type failedPrefaceConn struct{ net.Conn }

func (*failedPrefaceConn) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestConnectionMetricsDialAttemptsAndLockFreeDelivery(t *testing.T) {
	for _, stage := range []string{"success", "dial", "tls", "transport"} {
		t.Run(stage, func(t *testing.T) {
			slot := new(transportSlot)
			t.Cleanup(slot.close)
			observer := &poolDialObserver{hook: func() {
				if !slot.mu.TryLock() {
					t.Error("dial callback ran under slot creation lock")
					return
				}
				slot.mu.Unlock()
			}}
			opts := slotTestOpts
			opts.ConnectionObserver, opts.ConnectionPoolID = observer, 123
			dialer := observerPipeDialer(t)
			wantOutcome, wantReason := connstats.DialEstablished, connstats.ReasonNone
			switch stage {
			case "dial":
				dialer = newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) { return nil, errors.New("dial failed") })
				wantOutcome, wantReason = connstats.DialFailed, connstats.ReasonOther
			case "tls":
				client, server := net.Pipe()
				_ = server.Close()
				t.Cleanup(func() { _ = client.Close() })
				dialer = newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) { return client, nil })
				opts.TLSConfig = &tls.Config{InsecureSkipVerify: true}
				wantOutcome, wantReason = connstats.DialTLSFailed, connstats.ReasonNetwork
			case "transport":
				client, server := net.Pipe()
				t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
				dialer = newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) { return &failedPrefaceConn{Conn: client}, nil })
				wantOutcome, wantReason = connstats.DialTransportFailed, connstats.ReasonNetwork
			}
			tr, err := slot.createTransport("metrics-service", dialer, "tcp", mockAddr0, time.Second, opts)
			if stage == "success" {
				if err != nil {
					t.Fatal(err)
				}
				again, err := slot.createTransport("metrics-service", dialer, "tcp", mockAddr0, time.Second, opts)
				if err != nil || again != tr {
					t.Fatalf("pool hit = (%v, %v)", again, err)
				}
			} else if err == nil {
				t.Fatal("failing establishment stage succeeded")
			}
			events, metas := observer.snapshot()
			if len(events) != 1 || events[0].Outcome != wantOutcome || events[0].Reason != wantReason || events[0].Duration <= 0 {
				t.Fatalf("dial outcomes = %+v", events)
			}
			if metas[0].Service != "metrics-service" || metas[0].PoolID != 123 || metas[0].Target != mockAddr0 {
				t.Fatalf("dial metadata = %+v", metas)
			}
		})
	}
}

func TestConnectionMetricsDialHookPanicDoesNotChangeFailure(t *testing.T) {
	sentinel := errors.New("dial sentinel")
	observer := &poolDialObserver{hook: func() { panic("broken dial hook") }}
	opts := slotTestOpts
	opts.ConnectionObserver = observer
	_, err := newTransport("metrics-service", newMockDialerWithDialFunc(func(string, string, time.Duration) (net.Conn, error) {
		return nil, sentinel
	}), "tcp", mockAddr0, time.Second, opts, nil, nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("dial hook changed original failure: %v", err)
	}
}
