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
	"math"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/cloudwego/kitex/pkg/remote/connstats"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/grpc/grpcframe"
)

type connectionMetricsObserver struct{ factory connectionObserverFunc }

func (o connectionMetricsObserver) NewConnection(meta connstats.Meta) connstats.Recorder {
	return o.factory(meta)
}

// A marker must never be invoked under a transport creation lock.
func (connectionMetricsObserver) ConnectionMetricsEnabled() { panic("marker invoked") }

type connectionMetricsRecorder struct {
	connectionEventRecorder
	streamMu sync.Mutex
	stream   connstats.StreamEvent
}

func (r *connectionMetricsRecorder) ObserveStreams(event connstats.StreamEvent) {
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	if event.Version > r.stream.Version && !r.stream.Terminal {
		r.stream = event
	}
}

func (r *connectionMetricsRecorder) streamSnapshot() connstats.StreamEvent {
	r.streamMu.Lock()
	defer r.streamMu.Unlock()
	return r.stream
}

func newMetricsTransport(t *testing.T, r *connectionMetricsRecorder) *http2Client {
	t.Helper()
	conn := newConnectionObserverTestConn()
	conn.headers = make(chan struct{}, 4)
	tr, err := NewClientTransportWithConfig(context.Background(), conn, ConnectOptions{
		ConnectionObserver: connectionMetricsObserver{factory: func(connstats.Meta) connstats.Recorder { return r }},
	}, ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	client := tr.(*http2Client)
	client.ActivateConnectionObserver()
	t.Cleanup(func() {
		_ = client.Close(ErrConnClosing)
		awaitConnectionSignal(t, client.readerDone)
		awaitConnectionSignal(t, client.writerDone)
	})
	return client
}

func TestConnectionMetricsStreamLifecycleAndGoAway(t *testing.T) {
	r := new(connectionMetricsRecorder)
	client := newMetricsTransport(t, r)
	first, err := client.NewStream(context.Background(), &CallHdr{Method: "/service/method"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.NewStream(context.Background(), &CallHdr{Method: "/service/method"})
	if err != nil {
		t.Fatal(err)
	}
	if event := r.streamSnapshot(); event.Active != 2 || event.Terminal {
		t.Fatalf("active stream snapshot = %+v", event)
	}
	awaitConnectionSignal(t, client.conn.(*connectionObserverTestConn).headers)
	awaitConnectionSignal(t, client.conn.(*connectionObserverTestConn).headers)
	client.handleGoAway(&grpcframe.GoAwayFrame{LastStreamID: first.id, ErrCode: http2.ErrCodeNo})
	awaitConnectionSignal(t, second.done)
	client.handleGoAway(&grpcframe.GoAwayFrame{LastStreamID: first.id, ErrCode: http2.ErrCodeNo})
	if event := r.streamSnapshot(); event.Active != 1 {
		t.Fatalf("after GOAWAY rejected stream = %+v", event)
	}
	client.CloseStream(first, nil)
	awaitConnectionSignal(t, client.ctx.Done())
	awaitConnectionSignal(t, client.readerDone)
	latest, _ := r.snapshot()
	if latest.State != connstats.StateClosed || latest.GoAwayReceived != 2 || latest.CloseReason != connstats.ReasonGoAway {
		t.Fatalf("closed lifecycle = %+v", latest)
	}
	if latest.EstablishedAt.IsZero() || latest.DrainingAt.Before(latest.EstablishedAt) || latest.ClosedAt.Before(latest.DrainingAt) {
		t.Fatalf("invalid absolute lifecycle times = %+v", latest)
	}
	if !latest.Streams.Terminal || latest.Streams.Active != 0 || latest.Streams.Waiting != 0 || latest.Streams.Capacity != 0 {
		t.Fatalf("terminal stream gauges = %+v", latest.Streams)
	}
}

func TestConnectionMetricsQuotaWaitCancellationAndTerminalSnapshot(t *testing.T) {
	r := new(connectionMetricsRecorder)
	client := newMetricsTransport(t, r)
	// The mock peer's initial SETTINGS must complete before overriding its
	// quota. Otherwise the genuine reader may overwrite the test's zero limit.
	deadline := time.Now().Add(3 * time.Second)
	for {
		client.mu.Lock()
		ready := client.observation.streams.Capacity == math.MaxUint32
		client.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("initial peer SETTINGS were not processed")
		}
		time.Sleep(time.Millisecond)
	}
	// SETTINGS are serialized with NewStream by the control buffer. The
	// production wire test also exercises peer SETTINGS/ACK in the pool suite.
	_, err := client.controlBuf.executeAndPut(func(interface{}) bool {
		client.streamQuota, client.maxConcurrentStreams = 0, 0
		client.observationCapacity(0)
		return true
	}, &incomingSettings{})
	if err != nil {
		t.Fatal(err)
	}
	client.publishStreamObservation()
	startWait := func() (context.CancelFunc, <-chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := client.NewStream(ctx, &CallHdr{Method: "/service/method"}); done <- err }()
		deadline := time.After(3 * time.Second)
		for r.streamSnapshot().Waiting != 1 {
			select {
			case <-deadline:
				t.Fatalf("quota waiter was not observed: %+v", r.streamSnapshot())
			default:
				time.Sleep(time.Millisecond)
			}
		}
		return cancel, done
	}
	cancel, done := startWait()
	cancel()
	if err := <-done; err == nil {
		t.Fatal("canceled quota waiter succeeded")
	}
	first := r.streamSnapshot()
	if first.Waiting != 0 || first.QuotaWaitCount != 1 || first.QuotaWaitDuration <= 0 {
		t.Fatalf("completed canceled wait = %+v", first)
	}
	cancel, done = startWait()
	defer cancel()
	client.Close(ErrConnClosing)
	if err := <-done; err == nil {
		t.Fatal("closed quota waiter succeeded")
	}
	latest, _ := r.snapshot()
	if !latest.Streams.Terminal || latest.Streams.Waiting != 0 || latest.Streams.QuotaWaitCount != 2 || latest.Streams.QuotaWaitDuration < first.QuotaWaitDuration {
		t.Fatalf("terminal snapshot did not finalize waiters = %+v", latest.Streams)
	}
	client.mu.Lock()
	if len(client.observation.waits) != 0 || client.observation.streams.QuotaWaitCount != 2 {
		t.Error("closed observation retained waiters or counted one twice")
	}
	client.mu.Unlock()
}

func TestConnectionMetricsFirstEventClosedHasHistory(t *testing.T) {
	r := new(connectionMetricsRecorder)
	entered, release, activated := make(chan struct{}), make(chan struct{}), make(chan struct{})
	tr, err := NewClientTransportWithConfig(context.Background(), newConnectionObserverTestConn(), ConnectOptions{
		ConnectionObserver: connectionMetricsObserver{factory: func(connstats.Meta) connstats.Recorder {
			close(entered)
			<-release
			return r
		}},
	}, ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	client := tr.(*http2Client)
	go func() { client.ActivateConnectionObserver(); close(activated) }()
	awaitConnectionSignal(t, entered)
	stream, err := client.NewStream(context.Background(), &CallHdr{Method: "/service/method"})
	if err != nil {
		t.Fatal(err)
	}
	client.handleGoAway(&grpcframe.GoAwayFrame{LastStreamID: stream.id})
	client.Close(ErrConnClosing)
	close(release)
	awaitConnectionSignal(t, activated)
	latest, events := r.snapshot()
	if len(events) != 1 || latest.State != connstats.StateClosed || latest.GoAwayReceived != 1 || latest.EstablishedAt.IsZero() || latest.DrainingAt.IsZero() || latest.ClosedAt.IsZero() || !latest.Streams.Terminal {
		t.Fatalf("first closed event lost lifecycle history: %+v, deliveries=%d", latest, len(events))
	}
	awaitConnectionSignal(t, client.readerDone)
	awaitConnectionSignal(t, client.writerDone)
}

func TestConnectionMetricsQuotaDurationDoesNotOverflow(t *testing.T) {
	o := &connectionObservation{streams: connstats.StreamEvent{QuotaWaitDuration: time.Duration(math.MaxInt64 - 1)}}
	for i := 0; i < 2; i++ {
		wait := &quotaWaitObservation{started: time.Now().Add(-time.Second)}
		o.completeQuotaWaitLocked(wait, time.Now())
	}
	if o.streams.QuotaWaitDuration != time.Duration(math.MaxInt64) || o.streams.QuotaWaitCount != 2 {
		t.Fatalf("cumulative quota wait wrapped: %+v", o.streams)
	}
}

func TestConnectionMetricsCloseReasonsForContextStatuses(t *testing.T) {
	for _, test := range []struct {
		err    error
		reason connstats.Reason
	}{
		{context.Canceled, connstats.ReasonCanceled},
		{context.DeadlineExceeded, connstats.ReasonDeadline},
		{cascadeContextErr(context.Canceled), connstats.ReasonCanceled},
		{cascadeContextErr(context.DeadlineExceeded), connstats.ReasonDeadline},
		{ErrConnClosing, connstats.ReasonLocal},
	} {
		if got := connectionCloseReason(test.err); got != test.reason {
			t.Errorf("reason for %v = %v, want %v", test.err, got, test.reason)
		}
	}
}

func TestConnectionMetricsQuotaWaitEndsAtAdmission(t *testing.T) {
	r := new(connectionMetricsRecorder)
	client := newMetricsTransport(t, r)
	// Wait until initial SETTINGS are consumed before setting a zero quota.
	deadline := time.Now().Add(3 * time.Second)
	for {
		client.mu.Lock()
		ready := client.observation.streams.Capacity == math.MaxUint32
		client.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("initial SETTINGS not processed")
		}
		time.Sleep(time.Millisecond)
	}
	_, _ = client.controlBuf.executeAndPut(func(interface{}) bool {
		client.streamQuota, client.maxConcurrentStreams = 0, 0
		client.observationCapacity(0)
		return true
	}, &incomingSettings{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	streamCh, errCh := make(chan *Stream, 1), make(chan error, 1)
	go func() {
		stream, err := client.NewStream(ctx, &CallHdr{Method: "/service/method"})
		streamCh <- stream
		errCh <- err
	}()
	for r.streamSnapshot().Waiting != 1 {
		select {
		case <-ctx.Done():
			t.Fatal("quota waiter not observed")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	_, _ = client.controlBuf.executeAndPut(func(interface{}) bool {
		client.streamQuota, client.maxConcurrentStreams = 1, 1
		client.observationCapacity(1)
		client.streamsQuotaAvailable <- struct{}{}
		return true
	}, &incomingSettings{})
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	stream := <-streamCh
	if event := r.streamSnapshot(); event.Waiting != 0 || event.Active != 1 || event.QuotaWaitCount != 1 || event.QuotaWaitDuration <= 0 {
		t.Fatalf("quota admission lost completed wait: %+v", event)
	}
	client.CloseStream(stream, nil)
}

func TestConnectionMetricsBlockingTerminalAfterAllStreamsCleaned(t *testing.T) {
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	tr, err := NewClientTransportWithConfig(context.Background(), newConnectionObserverTestConn(), ConnectOptions{
		ConnectionObserver: connectionMetricsObserver{factory: func(connstats.Meta) connstats.Recorder {
			return connectionRecorderFunc(func(event connstats.Event) {
				if event.State == connstats.StateClosed {
					once.Do(func() { close(entered); <-release })
				}
			})
		}},
	}, ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	client := tr.(*http2Client)
	client.ActivateConnectionObserver()
	var streams []*Stream
	for i := 0; i < 3; i++ {
		stream, err := client.NewStream(context.Background(), &CallHdr{Method: "/service/method"})
		if err != nil {
			t.Fatal(err)
		}
		streams = append(streams, stream)
	}
	go func() { _ = client.Close(ErrConnClosing); close(done) }()
	awaitConnectionSignal(t, entered)
	awaitConnectionSignal(t, client.ctx.Done())
	awaitConnectionSignal(t, client.conn.(*connectionObserverTestConn).closed)
	for _, stream := range streams {
		awaitConnectionSignal(t, stream.done)
	}
	close(release)
	awaitConnectionSignal(t, done)
	awaitConnectionSignal(t, client.readerDone)
	awaitConnectionSignal(t, client.writerDone)
}

func BenchmarkConnectionMetricsStreamAccounting(b *testing.B) {
	for _, mode := range []string{"disabled", "state-only", "detailed"} {
		b.Run(mode, func(b *testing.B) {
			client := new(http2Client)
			factory := connectionObserverFunc(func(connstats.Meta) connstats.Recorder { return nil })
			switch mode {
			case "state-only":
				client.observation = newConnectionObservation(factory, connstats.Meta{})
			case "detailed":
				client.observation = newConnectionObservation(connectionMetricsObserver{factory: factory}, connstats.Meta{})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				client.mu.Lock()
				client.observationStreamOpenedLocked()
				client.mu.Unlock()
				client.observationStreamClosed()
				client.publishStreamObservation()
			}
		})
	}
}

type connectionMetricsNoopRecorder struct{}

func (connectionMetricsNoopRecorder) Observe(connstats.Event)              {}
func (connectionMetricsNoopRecorder) ObserveStreams(connstats.StreamEvent) {}

func BenchmarkConnectionMetricsStreamLifecycle(b *testing.B) {
	for _, mode := range []string{"disabled", "state-only", "detailed"} {
		b.Run(mode, func(b *testing.B) {
			opts := ConnectOptions{}
			factory := connectionObserverFunc(func(connstats.Meta) connstats.Recorder { return connectionMetricsNoopRecorder{} })
			switch mode {
			case "state-only":
				opts.ConnectionObserver = factory
			case "detailed":
				opts.ConnectionObserver = connectionMetricsObserver{factory: factory}
			}
			tr, err := NewClientTransportWithConfig(context.Background(), newConnectionObserverTestConn(), opts, ClientConfig{})
			if err != nil {
				b.Fatal(err)
			}
			client := tr.(*http2Client)
			client.ActivateConnectionObserver()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stream, err := client.NewStream(context.Background(), &CallHdr{Method: "/service/method"})
				if err != nil {
					b.Fatal(err)
				}
				client.CloseStream(stream, nil)
			}
			b.StopTimer()
			_ = client.Close(ErrConnClosing)
			<-client.readerDone
			<-client.writerDone
		})
	}
}
