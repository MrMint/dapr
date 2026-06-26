/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package callback

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	nethttp "net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/dapr/tests/integration/framework"
	"github.com/dapr/dapr/tests/integration/framework/client"
	"github.com/dapr/dapr/tests/integration/framework/process/daprd/actors"
	"github.com/dapr/dapr/tests/integration/suite"
)

func init() {
	suite.Register(new(rebalance))
}

// rebalance covers a timer whose actor is moved to another host after the timer
// was registered. Timers are host-local and not durable, so the registering
// host drops the timer on rebalance rather than routing it cross-host; the new
// host never receives the fire.
type rebalance struct {
	app1 *actors.Actors
	app2 *actors.Actors

	done atomic.Bool
	// timer1 counts fires delivered to app1 (local, asserted positive). timer2
	// counts fires delivered to app2, which must stay zero: a rebalanced actor's
	// timer is dropped, not forwarded.
	timer1, timer2 atomic.Int64
}

func (h *rebalance) Setup(t *testing.T) []framework.Option {
	// newHandler records timer fires for one host and asserts every fire
	// carries the callback the timer was registered with.
	newHandler := func(count *atomic.Int64) nethttp.HandlerFunc {
		return func(_ nethttp.ResponseWriter, r *nethttp.Request) {
			if h.done.Load() || !strings.HasSuffix(r.URL.Path, "/method/timer/foo") {
				return
			}
			assert.Equal(t, nethttp.MethodPut, r.Method)
			b, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			var payload struct {
				Callback string `json:"callback"`
			}
			assert.NoError(t, json.Unmarshal(b, &payload))
			assert.Equal(t, "mycallback", payload.Callback)
			count.Add(1)
		}
	}

	h.app1 = actors.New(t,
		actors.WithActorTypes("abc"),
		actors.WithActorTypeHandler("abc", newHandler(&h.timer1)),
	)

	h.app2 = actors.New(t,
		actors.WithPeerActor(h.app1),
		actors.WithActorTypes("abc"),
		actors.WithActorTypeHandler("abc", newHandler(&h.timer2)),
	)

	// Only app1 is started here; app2 is brought up mid-test to trigger a
	// rebalance while app1 (which holds the timers) stays alive.
	return []framework.Option{
		framework.WithProcesses(h.app1),
	}
}

func (h *rebalance) Run(t *testing.T, ctx context.Context) {
	// Stop asserting in the handlers once the test body returns, since timers
	// keep firing until the daprd processes are torn down.
	defer h.done.Store(true)

	h.app1.WaitUntilRunning(t, ctx)

	client := client.HTTP(t)

	body := `{
"dueTime": "0s",
"period": "1s",
"data": "hello",
"callback": "mycallback"
}`

	// Register a recurring timer with a callback on a batch of actors. With
	// app1 the only host, every actor (and therefore its timer) lives on app1.
	// Using a batch guarantees that, once app2 joins, at least one of these
	// actors rebalances onto app2.
	const numActors = 30
	for i := range numActors {
		id := strconv.Itoa(i)

		// Activate the actor so the timer can be created.
		url := fmt.Sprintf("http://%s/v1.0/actors/abc/%s/method/foo", h.app1.Daprd().HTTPAddress(), id)
		req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, url, nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		url = fmt.Sprintf("http://%s/v1.0/actors/abc/%s/timers/foo", h.app1.Daprd().HTTPAddress(), id)
		req, err = nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, url, strings.NewReader(body))
		require.NoError(t, err)
		resp, err = client.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, nethttp.StatusNoContent, resp.StatusCode)
	}

	// Sanity check: timers fire locally on app1 with the callback intact, and
	// app1 holds all of the timers it just registered.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Positive(c, h.timer1.Load())
		ms := h.app1.Daprd().Metrics(c, ctx).MatchMetric("dapr_runtime_actor_timers|", "actor_type:abc")
		require.Len(c, ms, 1)
		assert.Equal(c, float64(numActors), ms[0].Value)
	}, time.Second*10, time.Millisecond*10)

	// Bring app2 online. Placement re-disseminates and roughly half the actors
	// rebalance onto app2; app1 drops the timers for those actors, so its
	// active-timer count falls below the number it registered.
	h.app2.Run(t, ctx)
	t.Cleanup(func() { h.app2.Cleanup(t) })
	h.app2.WaitUntilRunning(t, ctx)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ms := h.app1.Daprd().Metrics(c, ctx).MatchMetric("dapr_runtime_actor_timers|", "actor_type:abc")
		require.Len(c, ms, 1)
		assert.Less(c, ms[0].Value, float64(numActors))
	}, time.Second*30, time.Millisecond*100)

	// Timers are never routed cross-host, so app2 must not receive a fire for any
	// rebalanced actor's timer.
	assert.Equal(t, int64(0), h.timer2.Load())

	// app1 keeps firing the actors it still owns.
	assert.Positive(t, h.timer1.Load())
}
