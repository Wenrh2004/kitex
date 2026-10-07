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
	"testing"

	internalclient "github.com/cloudwego/kitex/internal/client"
	"github.com/cloudwego/kitex/pkg/remote/connstats"
	"github.com/cloudwego/kitex/pkg/remote/trans/nphttp2/grpc"
	"github.com/cloudwego/kitex/pkg/utils"
)

type optionConnectionObserver struct{ id int }

func (*optionConnectionObserver) NewConnection(connstats.Meta) connstats.Recorder { return nil }

func TestWithGRPCConnectionObserver(t *testing.T) {
	defaults := internalclient.NewOptions(nil)
	t.Cleanup(func() { _ = defaults.RemoteOpt.ConnPool.Close() })
	if defaults.GRPCConnectOpts.ConnectionObserver != nil {
		t.Fatal("connection observation must be disabled by default")
	}

	opts := &internalclient.Options{GRPCConnectOpts: &grpc.ConnectOptions{}}
	var debug utils.Slice
	first := &optionConnectionObserver{id: 1}
	second := &optionConnectionObserver{id: 2}
	WithGRPCConnectionObserver(first).F(opts, &debug)
	if opts.GRPCConnectOpts.ConnectionObserver != first {
		t.Fatal("observer option was not applied")
	}
	if len(debug) != 1 || debug[0] != "WithGRPCConnectionObserver" {
		t.Fatalf("unexpected option debug info: %v", debug)
	}
	WithGRPCConnectionObserver(second).F(opts, &debug)
	if opts.GRPCConnectOpts.ConnectionObserver != second {
		t.Fatal("the last observer option must replace the previous one")
	}
	WithGRPCConnectionObserver(nil).F(opts, &debug)
	if opts.GRPCConnectOpts.ConnectionObserver != nil {
		t.Fatal("nil observer must disable observation")
	}
}
