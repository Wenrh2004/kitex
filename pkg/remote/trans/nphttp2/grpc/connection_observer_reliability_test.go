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
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote/connstats"
)

type observerWarningLogger struct {
	klog.FullLogger
	calls      atomic.Int32
	panicValue interface{}
	panics     bool
}

func (l *observerWarningLogger) Warnf(format string, args ...interface{}) {
	l.calls.Add(1)
	_ = fmt.Sprintf(format, args...)
	if l.panics {
		panic(l.panicValue)
	}
}

type observerPanicValue struct{ formatted atomic.Int32 }

func (v *observerPanicValue) String() string {
	v.formatted.Add(1)
	return "arbitrary observer panic value"
}

type observerPanicMetricsRecorder struct{ value interface{} }

func (r observerPanicMetricsRecorder) ObserveStreams(connstats.StreamEvent) { panic(r.value) }

func TestConnectionObserverWarningIsolation(t *testing.T) {
	if runIsolatedObserverWarningTest(t) {
		return
	}
	// SetLogger is process-global and not concurrent-safe. These helper tests
	// start no transport goroutines and deliberately do not use t.Parallel.
	previous := klog.DefaultLogger()
	t.Cleanup(func() { klog.SetLogger(previous) })
	for _, nilPanic := range []bool{false, true} {
		for _, loggerPanics := range []bool{false, true} {
			for _, operation := range []string{"factory", "state", "streams"} {
				t.Run(fmt.Sprintf("%s/nil=%v/logger_panic=%v", operation, nilPanic, loggerPanics), func(t *testing.T) {
					payload := new(observerPanicValue)
					var value interface{} = payload
					if nilPanic {
						value = nil
					}
					logger := &observerWarningLogger{FullLogger: previous, panics: loggerPanics, panicValue: value}
					klog.SetLogger(logger)
					connectionObserverLastWarning.Store(0)
					run := func() {
						switch operation {
						case "factory":
							r := safeNewConnection(connectionObserverFunc(func(connstats.Meta) connstats.Recorder { panic(value) }), connstats.Meta{})
							if r != nil {
								t.Fatal("panicking factory retained a recorder")
							}
						case "state":
							safeObserveConnection(connectionRecorderFunc(func(connstats.Event) { panic(value) }), connstats.Event{})
						case "streams":
							safeObserveStreams(observerPanicMetricsRecorder{value: value}, connstats.StreamEvent{})
						}
					}
					run()
					run()
					if logger.calls.Load() != 1 {
						t.Fatalf("warning calls = %d, want one limited warning", logger.calls.Load())
					}
					if payload.formatted.Load() != 0 {
						t.Fatal("warning invoked arbitrary panic payload formatter")
					}
				})
			}
		}
	}
}

func runIsolatedObserverWarningTest(t *testing.T) bool {
	t.Helper()
	const marker = "KITEX_GRPC_OBSERVER_WARNING_TEST"
	if os.Getenv(marker) == t.Name() {
		return false
	}
	// Other transport tests may still be joining asynchronous writers. Use
	// this same test binary (including race instrumentation) to exercise the
	// intentionally broken process-global logger without racing those tests.
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.timeout=25s")
	if coverDir := flag.Lookup("test.gocoverdir"); coverDir != nil && coverDir.Value.String() != "" {
		// Share counter pods with the parent without overwriting its profile.
		cmd.Args = append(cmd.Args, "-test.gocoverdir="+coverDir.Value.String())
	}
	cmd.Env = append(os.Environ(), marker+"="+t.Name())
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("isolated observer warning test: %v\n%s", err, output)
	}
	return true
}
