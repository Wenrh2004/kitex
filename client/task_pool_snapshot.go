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

import "github.com/cloudwego/kitex/pkg/taskstats"

// RPCTimeoutPoolSnapshot reports the shared unary timeout pool's reusable
// workers and idle retention settings. Overflow goroutines are not workers in
// this pool. Its handoff channel is unbuffered; MaxIdle is not a hard concurrency
// limit. Export this snapshot once per process, without service labels.
func RPCTimeoutPoolSnapshot() taskstats.PoolSnapshot { return workerPool.snapshot() }

func (p *timeoutPool) snapshot() taskstats.PoolSnapshot {
	return taskstats.PoolSnapshot{
		Workers: int64(p.Size()), WorkersKnown: true,
		QueueCapacity: int64(cap(p.tasks)), QueueCapacityKnown: true,
		MaxIdle: int64(p.maxIdle), MaxIdleKnown: true,
		MaxIdleTime: p.maxIdleTime, MaxIdleTimeKnown: true,
	}
}
