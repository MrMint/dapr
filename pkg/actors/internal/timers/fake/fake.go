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

package fake

import (
	"context"

	"github.com/dapr/dapr/pkg/actors/api"
	"github.com/dapr/dapr/pkg/actors/internal/timers"
)

type Fake struct {
	createFn          func(context.Context, *api.Reminder) error
	deleteFn          func(context.Context, string)
	deleteForActorsFn func(context.Context, string, []string)
	closeFn           func() error
}

func New() *Fake {
	return &Fake{
		createFn:          func(context.Context, *api.Reminder) error { return nil },
		deleteFn:          func(context.Context, string) {},
		deleteForActorsFn: func(context.Context, string, []string) {},
		closeFn:           func() error { return nil },
	}
}

func (f *Fake) WithCreateFn(fn func(context.Context, *api.Reminder) error) *Fake {
	f.createFn = fn
	return f
}

func (f *Fake) WithDeleteFn(fn func(context.Context, string)) *Fake {
	f.deleteFn = fn
	return f
}

func (f *Fake) WithDeleteForActorsFn(fn func(context.Context, string, []string)) *Fake {
	f.deleteForActorsFn = fn
	return f
}

func (f *Fake) WithCloseFn(fn func() error) *Fake {
	f.closeFn = fn
	return f
}

func (f *Fake) Create(ctx context.Context, reminder *api.Reminder) error {
	return f.createFn(ctx, reminder)
}

func (f *Fake) Delete(ctx context.Context, timerKey string) {
	f.deleteFn(ctx, timerKey)
}

func (f *Fake) DeleteForActors(ctx context.Context, actorType string, actorIDs []string) {
	f.deleteForActorsFn(ctx, actorType, actorIDs)
}

func (f *Fake) Close() error {
	return f.closeFn()
}

var _ timers.Storage = (*Fake)(nil)
