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
	internalclient "github.com/cloudwego/kitex/internal/client"
	"github.com/cloudwego/kitex/pkg/taskstats"
	"github.com/cloudwego/kitex/pkg/utils"
)

// WithTaskObserver observes unary executions scheduled by the RPC timeout pool.
// Caller timeout/cancellation does not finish the task; actual endpoint exit does.
// It does not cover synchronous calls without a timeout/cancelable context,
// streaming calls, Netpoll workers or all goroutines. nil disables observation.
func WithTaskObserver(observer taskstats.Observer) Option {
	return Option{F: func(o *internalclient.Options, di *utils.Slice) {
		di.Push("WithTaskObserver")
		o.TaskObserver = observer
	}}
}
