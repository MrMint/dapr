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

package inmemory

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/dapr/pkg/actors/api"
	"github.com/dapr/dapr/pkg/actors/router"
	routerfake "github.com/dapr/dapr/pkg/actors/router/fake"
)

func key(actorID, name string) string {
	return api.Reminder{ActorType: "abc", ActorID: actorID, Name: name}.Key()
}

func TestDeleteForActors(t *testing.T) {
	store := New(Options{Router: routerfake.New()}).(*inmemory)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	// Timers far in the future so they stay queued without firing.
	due := time.Now().Add(time.Hour)
	create := func(actorID, name string) {
		require.NoError(t, store.Create(t.Context(), &api.Reminder{
			ActorType:      "abc",
			ActorID:        actorID,
			Name:           name,
			RegisteredTime: due,
			Period:         api.NewEmptyReminderPeriod(),
			IsTimer:        true,
		}))
	}

	// "foo" has two timers, "bar" and "baz" have one each.
	create("foo", "t1")
	create("foo", "t2")
	create("bar", "t1")
	create("baz", "t1")
	require.Equal(t, int64(4), store.GetActiveTimersCount("abc"))

	// Drop foo's and bar's timers in a single call; baz is untouched.
	store.DeleteForActors(t.Context(), "abc", []string{"foo", "bar"})

	require.Equal(t, int64(1), store.GetActiveTimersCount("abc"))
	for _, k := range []string{key("foo", "t1"), key("foo", "t2"), key("bar", "t1")} {
		_, ok := store.activeTimers.Load(k)
		require.False(t, ok, k)
	}
	_, ok := store.activeTimers.Load(key("baz", "t1"))
	require.True(t, ok)

	// Empty and unknown inputs are no-ops.
	store.DeleteForActors(t.Context(), "abc", nil)
	store.DeleteForActors(t.Context(), "abc", []string{"nope"})
	require.Equal(t, int64(1), store.GetActiveTimersCount("abc"))
}

// A timer deleted while one of its fires is still in flight must not be
// resurrected by the processor's re-enqueue when that fire completes.
func TestDeletedTimerDoesNotResurrectAfterFire(t *testing.T) {
	var fires atomic.Int64
	firing := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }

	router := routerfake.New().WithCallReminderFn(func(context.Context, *api.Reminder) error {
		fires.Add(1)
		select {
		case firing <- struct{}{}:
		default:
		}
		<-release
		return nil
	})

	store := New(Options{Router: router}).(*inmemory)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	t.Cleanup(releaseFn) // runs first: unblock any in-flight fire before Close

	period, err := api.NewReminderPeriod("100ms")
	require.NoError(t, err)
	require.NoError(t, store.Create(t.Context(), &api.Reminder{
		ActorType:      "abc",
		ActorID:        "foo",
		Name:           "t1",
		RegisteredTime: time.Now(),
		Period:         period,
		IsTimer:        true,
	}))

	// Wait for the first fire to be in flight, then delete the timer.
	select {
	case <-firing:
	case <-time.After(time.Second * 5):
		t.Fatal("timer never fired")
	}
	store.DeleteForActors(t.Context(), "abc", []string{"foo"})

	// Releasing the in-flight fire must not re-enqueue the deleted timer.
	releaseFn()
	time.Sleep(time.Millisecond * 500)
	require.Equal(t, int64(1), fires.Load())
}

// When the router reports the timer's actor has been rebalanced off this host,
// the timer is removed and not fired again.
func TestActorMovedRemovesTimer(t *testing.T) {
	var fires atomic.Int64
	rfake := routerfake.New().WithCallReminderFn(func(context.Context, *api.Reminder) error {
		fires.Add(1)
		return router.ErrTimerActorMoved
	})

	store := New(Options{Router: rfake}).(*inmemory)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	period, err := api.NewReminderPeriod("50ms")
	require.NoError(t, err)
	require.NoError(t, store.Create(t.Context(), &api.Reminder{
		ActorType:      "abc",
		ActorID:        "foo",
		Name:           "t1",
		RegisteredTime: time.Now(),
		Period:         period,
		IsTimer:        true,
	}))

	// The first fire reports the actor has moved, so the timer is dropped.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int64(0), store.GetActiveTimersCount("abc"))
	}, time.Second*5, time.Millisecond*10)

	// It must not keep firing once removed.
	n := fires.Load()
	time.Sleep(time.Millisecond * 300)
	assert.Equal(t, n, fires.Load())
}
