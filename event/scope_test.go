package event_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/event/eventtest"
	"github.com/grandper/go-eventide/internal/fixture"
)

func TestScope(t *testing.T) {
	somethingHappened := &fixture.SomethingHappened{}

	t.Run("should provide a publisher to send events", func(t *testing.T) {
		event.PrivateScope(func(ctx context.Context) {
			checker, err := eventtest.Subscribe(ctx)
			require.NoError(t, err)

			require.NoError(t, event.Publish(ctx, somethingHappened))
			assert.Equal(t, []event.Event{somethingHappened}, checker.Events())
		})

		event.Scope(context.Background(), func(ctx context.Context) {
			checker, err := eventtest.Subscribe(ctx)
			require.NoError(t, err)

			require.NoError(t, event.Publish(ctx, somethingHappened))
			assert.Equal(t, []event.Event{somethingHappened}, checker.Events())
		})
	})

	t.Run("should provide a scope with preregistered subscribers", func(t *testing.T) {
		event.PrivateScope(func(ctx context.Context) {
			require.NoError(t, event.Publish(ctx, somethingHappened))
		})
	})

	t.Run("should propagate events from child scope to parent scope", func(t *testing.T) {
		event.PrivateScope(func(ctx context.Context) {
			eventWasSent := false
			require.NoError(t, event.Handle(ctx, func(context.Context, event.Event) error {
				eventWasSent = true
				return nil
			}))
			event.Scope(ctx, func(ctx context.Context) {
				require.NoError(t, event.Publish(ctx, somethingHappened))
			})
			assert.True(t, eventWasSent)
		})
	})

	t.Run("should hand the child scope's context to the parent scope's subscribers", func(t *testing.T) {
		event.PrivateScope(func(ctx context.Context) {
			var handledValue any
			require.NoError(t, event.Handle(ctx, func(ctx context.Context, _ event.Event) error {
				handledValue = ctx.Value(testContextKey{})
				return nil
			}))
			event.Scope(ctx, func(ctx context.Context) {
				publishingCtx := context.WithValue(ctx, testContextKey{}, "value")
				require.NoError(t, event.Publish(publishingCtx, somethingHappened))
			})
			assert.Equal(t, "value", handledValue)
		})
	})

	t.Run("should create private child scope that don't send event to the parent scope", func(t *testing.T) {
		event.PrivateScope(func(ctx context.Context) {
			require.NoError(t, event.Handle(ctx, func(context.Context, event.Event) error {
				assert.Fail(t, "no event should reach this code")
				return nil
			}))
			event.PrivateScope(func(ctx context.Context) {
				eventWasSent := false
				require.NoError(t, event.Handle(ctx, func(context.Context, event.Event) error {
					eventWasSent = true
					return nil
				}))
				require.NoError(t, event.Publish(ctx, somethingHappened))
				assert.True(t, eventWasSent)
			})
		})
	})
}
