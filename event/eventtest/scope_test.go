package eventtest_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/event/eventtest"
	"github.com/grandper/go-eventide/internal/fixture"
)

type testContextKey struct{}

func TestScope(t *testing.T) {
	somethingHappened := fixture.NewSomethingHappened()
	somethingElseHappened := fixture.NewSomethingElseHappened()

	t.Run("no event happened", func(t *testing.T) {
		assert.True(t, eventtest.Scope(func(_ context.Context) {
		}).AssertEvents(t, nil))
	})

	t.Run("events are recorded in order", func(t *testing.T) {
		assert.True(t, eventtest.Scope(func(ctx context.Context) {
			require.NoError(t, event.Publish(ctx, somethingHappened))
			require.NoError(t, event.Publish(ctx, somethingElseHappened))
		}).AssertEvents(t, []event.Event{
			somethingHappened,
			somethingElseHappened,
		}))
	})

	mockT := new(testing.T)

	t.Run("more events occurred", func(t *testing.T) {
		assert.False(t, eventtest.Scope(func(ctx context.Context) {
			require.NoError(t, event.Publish(ctx, somethingHappened))
			require.NoError(t, event.Publish(ctx, somethingElseHappened))
		}).AssertEvents(mockT, []event.Event{
			somethingHappened,
		}))
	})

	t.Run("less events occurred", func(t *testing.T) {
		assert.False(t, eventtest.Scope(func(ctx context.Context) {
			require.NoError(t, event.Publish(ctx, somethingHappened))
		}).AssertEvents(mockT, []event.Event{
			somethingHappened,
			somethingElseHappened,
		}))
	})

	t.Run("wrong events occurred", func(t *testing.T) {
		assert.False(t, eventtest.Scope(func(ctx context.Context) {
			require.NoError(t, event.Publish(ctx, somethingHappened))
			require.NoError(t, event.Publish(ctx, somethingElseHappened))
		}).AssertEvents(mockT, []event.Event{
			somethingHappened,
			somethingHappened,
		}))
	})

	t.Run("the provided context is kept", func(t *testing.T) {
		parentCtx := context.WithValue(context.Background(), testContextKey{}, "value")
		var scopedValue any
		checker := eventtest.ScopeWithContext(parentCtx, func(ctx context.Context) {
			scopedValue = ctx.Value(testContextKey{})
			require.NoError(t, event.Publish(ctx, somethingHappened))
		})
		assert.Equal(t, "value", scopedValue)
		assert.True(t, checker.AssertEvents(t, []event.Event{somethingHappened}))
	})

	t.Run("extra subscribers receive the events too", func(t *testing.T) {
		extra := eventtest.NewChecker()
		subscribers := make([]event.Subscriber, 1, 2) // spare capacity must not be written into
		subscribers[0] = extra
		checker := eventtest.ScopeWithContext(context.Background(), func(ctx context.Context) {
			require.NoError(t, event.Publish(ctx, somethingHappened))
		}, subscribers...)
		assert.True(t, checker.AssertEvents(t, []event.Event{somethingHappened}))
		assert.True(t, extra.AssertEvents(t, []event.Event{somethingHappened}))
		assert.Len(t, subscribers[:2], 2)
		assert.Nil(t, subscribers[:2][1], "the caller's backing array is left untouched")
	})
}
