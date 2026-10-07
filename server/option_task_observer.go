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
	internalserver "github.com/cloudwego/kitex/internal/server"
	"github.com/cloudwego/kitex/pkg/taskstats"
	"github.com/cloudwego/kitex/pkg/utils"
)

// WithTaskObserver observes unary and streaming business handler execution.
// It starts at handler entry, so pending duration is not Netpoll scheduling or
// decoding latency. Tasks stay running until the actual handler returns.
// It does not instrument the network worker pool. nil disables observation.
func WithTaskObserver(observer taskstats.Observer) Option {
	return Option{F: func(o *internalserver.Options, di *utils.Slice) {
		di.Push("WithTaskObserver")
		o.TaskObserver = observer
	}}
}
