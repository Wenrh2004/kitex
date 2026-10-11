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
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cloudwego/kitex/pkg/remote/connstats"
)

type constructionReasonReadConn struct {
	*connectionObserverTestConn
	failure error
}

type connectionReasonCleanupConn struct {
	*connectionObserverTestConn
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (c *connectionReasonCleanupConn) Close() error {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return c.connectionObserverTestConn.Close()
}

func TestConnectionCloseReasonActivationWaitsForResourceCleanup(t *testing.T) {
	for _, detailed := range []bool{false, true} {
		name := "state-only"
		if detailed {
			name = "detailed"
		}
		t.Run(name, func(t *testing.T) {
			base := newConnectionObserverTestConn()
			release := make(chan struct{})
			conn := &connectionReasonCleanupConn{connectionObserverTestConn: base, entered: make(chan struct{}), release: release}
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			r := new(connectionEventRecorder)
			var observer connstats.Observer = connectionObserverFunc(func(connstats.Meta) connstats.Recorder { return r })
			if detailed {
				observer = connectionMetricsObserver{factory: func(connstats.Meta) connstats.Recorder { return r }}
			}
			tr, err := NewClientTransportWithConfig(context.Background(), conn, ConnectOptions{ConnectionObserver: observer}, ClientConfig{})
			if err != nil {
				unblock()
				t.Fatal(err)
			}
			client := tr.(*http2Client)
			closed := make(chan struct{})
			t.Cleanup(func() {
				unblock()
				_ = client.Close(ErrConnClosing)
				awaitConnectionSignal(t, closed)
				awaitConnectionSignal(t, client.readerDone)
				awaitConnectionSignal(t, client.writerDone)
			})
			go func() {
				defer close(closed)
				if err := client.Close(ErrConnClosing); err != nil {
					t.Error(err)
				}
			}()
			awaitConnectionSignal(t, conn.entered)
			// Close has committed its logical state, but the controlled socket
			// has not closed. First activation may install a recorder, not deliver
			// a terminal event before that cleanup completes.
			client.ActivateConnectionObserver()
			client.publishConnectionObservation()
			_, events := r.snapshot()
			for _, event := range events {
				if event.State == connstats.StateClosed {
					t.Error("activation published Closed before resource cleanup")
				}
			}
			unblock()
			awaitConnectionSignal(t, closed)
			latest, _ := r.snapshot()
			if latest.State != connstats.StateClosed || (detailed && latest.CloseReason != connstats.ReasonLocal) {
				t.Errorf("terminal event = %+v", latest)
			}
		})
	}
}

func (c *constructionReasonReadConn) Read(p []byte) (int, error) {
	n, err := c.connectionObserverTestConn.Read(p)
	if err == io.EOF {
		return n, c.failure
	}
	return n, err
}

func TestConnectionCloseReasonEarlyClosedConstructionActivatesFinalReason(t *testing.T) {
	conn := newConnectionObserverTestConn()
	conn.readEOF = true
	// The real reader closes the transport before the constructor's preface
	// write completes. The writable mock allows construction to return the
	// already logically closed transport, a supported observation boundary.
	conn.beforeWrite = func() { awaitConnectionSignal(t, conn.closed) }
	r := new(connectionMetricsRecorder)
	var client *http2Client
	var calls atomic.Int32
	failure := connectionReasonSafetyTimeoutError{&connectionReasonSafetyError{call: func() {
		calls.Add(1)
		if client == nil {
			t.Error("reader close classified error before construction returned")
			return
		}
		if client.IsActive() {
			t.Error("early-closed transport became active")
		}
	}}}
	tr, err := NewClientTransportWithConfig(context.Background(), &constructionReasonReadConn{connectionObserverTestConn: conn, failure: failure}, ConnectOptions{
		ConnectionObserver: connectionMetricsObserver{factory: func(connstats.Meta) connstats.Recorder { return r }},
	}, ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	client = tr.(*http2Client)
	t.Cleanup(func() {
		_ = client.Close(ErrConnClosing)
		awaitConnectionSignal(t, client.readerDone)
		awaitConnectionSignal(t, client.writerDone)
	})
	if calls.Load() != 0 {
		t.Fatal("unactivated construction called an application error method")
	}
	client.ActivateConnectionObserver()
	latest, _ := r.snapshot()
	if calls.Load() == 0 || latest.State != connstats.StateClosed || latest.CloseReason != connstats.ReasonNetwork || !latest.Streams.Terminal {
		t.Fatalf("early-closed activation lost original reason: calls=%d, event=%+v", calls.Load(), latest)
	}
}
