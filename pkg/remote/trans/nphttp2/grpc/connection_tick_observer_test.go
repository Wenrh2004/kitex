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
	"sync"
	"testing"

	"github.com/cloudwego/kitex/pkg/remote/connstats"
)

type tickStreamRecorder struct {
	mu       sync.Mutex
	calls    int
	latest   connstats.StreamEvent
	callback func(connstats.StreamEvent)
}

func (r *tickStreamRecorder) Observe(connstats.Event) {}
func (r *tickStreamRecorder) ObserveStreams(event connstats.StreamEvent) {
	r.mu.Lock()
	r.calls++
	if event.Version > r.latest.Version {
		r.latest = event
	}
	r.mu.Unlock()
	if r.callback != nil {
		r.callback(event)
	}
}

func tickObservationClient() *http2Client {
	ctx := context.Background()
	return &http2Client{
		ctx: ctx, scheme: "http", state: reachable,
		activeStreams: make(map[uint32]*Stream), controlBuf: newControlBuffer(ctx.Done()),
		nextID: 1, streamQuota: 100, maxConcurrentStreams: 100,
		streamsQuotaAvailable: make(chan struct{}, 1), bufferPool: newBufferPool(),
	}
}

func TestConnectionTickCatchesActualOrphanWithoutRepeatingSnapshot(t *testing.T) {
	r := new(tickStreamRecorder)
	client := tickObservationClient()
	r.callback = func(connstats.StreamEvent) {
		if !client.controlBuf.mu.TryLock() {
			t.Error("stream callback ran under control buffer lock")
			return
		}
		client.controlBuf.mu.Unlock()
	}
	client.observation = newConnectionObservation(connectionMetricsObserver{factory: func(connstats.Meta) connstats.Recorder { return r }}, connstats.Meta{})
	client.ActivateConnectionObserver()
	t.Cleanup(func() { client.controlBuf.finish(ErrConnClosing) })
	stream, err := client.NewStream(context.Background(), &CallHdr{Method: "/service/method"})
	if err != nil {
		t.Fatal(err)
	}
	if r.calls != 1 || r.latest.Active != 1 {
		t.Fatalf("initial stream observation = %d, %+v", r.calls, r.latest)
	}
	// This invokes the actual NewStream onOrphaned closure under controlBuf.mu.
	// There is no writer in this fixture, so the queued header is deterministic.
	client.controlBuf.finish(ErrConnClosing)
	awaitConnectionSignal(t, stream.done)
	if r.calls != 1 {
		t.Fatal("orphan cleanup synchronously delivered while holding controlBuf.mu")
	}
	task := &closeStreamTask{t: client}
	task.Tick()
	if r.calls != 2 || r.latest.Active != 0 {
		t.Fatalf("ticker missed orphan snapshot = %d, %+v", r.calls, r.latest)
	}
	for i := 0; i < 100; i++ {
		task.Tick()
	}
	if r.calls != 2 {
		t.Fatalf("unchanged tick repeated callback %d times", r.calls)
	}
}

func TestConnectionTickWatermarkDoesNotClaimMissingRecorderOrRetryPanic(t *testing.T) {
	r := new(tickStreamRecorder)
	client := tickObservationClient()
	client.observation = newConnectionObservation(connectionMetricsObserver{factory: func(connstats.Meta) connstats.Recorder { return r }}, connstats.Meta{})
	task := &closeStreamTask{t: client}
	task.Tick()
	if client.observation.streamAttempted != 0 {
		t.Fatal("ticker claimed a snapshot before factory activation")
	}
	client.ActivateConnectionObserver()
	task.Tick()
	if r.calls != 1 {
		t.Fatal("ticker did not explicitly deliver the initial stream snapshot")
	}
	r.callback = func(connstats.StreamEvent) { panic("partially recorded") }
	client.mu.Lock()
	client.observationStreamOpenedLocked()
	client.mu.Unlock()
	task.Tick()
	task.Tick()
	if r.calls != 2 {
		t.Fatal("ticker retried a partially recorded failing callback")
	}
}

func TestConnectionTickDoesNotAssumeRepeatedLifecycleConsumesNewStreams(t *testing.T) {
	r := new(tickStreamRecorder)
	client := tickObservationClient()
	client.observation = newConnectionObservation(connectionMetricsObserver{factory: func(connstats.Meta) connstats.Recorder { return r }}, connstats.Meta{})
	client.ActivateConnectionObserver()
	task := &closeStreamTask{t: client}
	task.Tick()
	client.mu.Lock()
	client.observationStreamOpenedLocked()
	client.mu.Unlock()
	client.publishStreamObservation()
	// Writer orphan cleanup changes streams without changing lifecycle Version.
	client.observationStreamClosed()
	client.publishConnectionObservation()
	client.publishConnectionObservation()
	if r.latest.Active != 1 {
		t.Fatal("fixture unexpectedly consumed embedded lifecycle streams")
	}
	task.Tick()
	if r.latest.Active != 0 || r.calls != 3 {
		t.Fatalf("new streams embedded in duplicate lifecycle blocked catch-up: %d, %+v", r.calls, r.latest)
	}
	task.Tick()
	if r.calls != 3 {
		t.Fatal("ticker repeated an unchanged catch-up")
	}
}

func BenchmarkConnectionTickIdle(b *testing.B) {
	for _, detailed := range []bool{false, true} {
		name := "disabled"
		if detailed {
			name = "detailed"
		}
		b.Run(name, func(b *testing.B) {
			client := tickObservationClient()
			if detailed {
				client.observation = newConnectionObservation(connectionMetricsObserver{factory: func(connstats.Meta) connstats.Recorder { return connectionMetricsNoopRecorder{} }}, connstats.Meta{})
				client.ActivateConnectionObserver()
			}
			task := &closeStreamTask{t: client}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				task.Tick()
			}
		})
	}
}
