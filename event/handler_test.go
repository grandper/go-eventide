package event_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/internal/fixture"
)

func TestHandler(t *testing.T) {
	t.Run("should handle event of the matching type", func(t *testing.T) {
		ctx := context.Background()
		p := event.NewPublisher()
		ctxWithPublisher := event.ContextWithPublisher(ctx, p)

		shouldBeActivated := false
		assert.NoError(t, event.Handle(ctxWithPublisher, func(context.Context, *fixture.SomethingHappened) error {
			shouldBeActivated = true
			return nil
		}))

		assert.NoError(t, event.Publish(ctxWithPublisher, &fixture.SomethingHappened{}))
		assert.True(t, shouldBeActivated)
	})

	t.Run("should ignore event with different types", func(t *testing.T) {
		ctx := context.Background()
		p := event.NewPublisher()
		ctxWithPublisher := event.ContextWithPublisher(ctx, p)

		shouldNotBeActivated := false
		assert.NoError(t, event.Handle(ctxWithPublisher, func(context.Context, *fixture.SomethingHappened) error {
			shouldNotBeActivated = true
			return nil
		}))

		assert.NoError(t, event.Publish(ctxWithPublisher, &fixture.SomethingElseHappened{}))
		assert.False(t, shouldNotBeActivated)
	})

	t.Run("should hand the publishing context to the handling function", func(t *testing.T) {
		p := event.NewPublisher()
		ctxWithPublisher := event.ContextWithPublisher(context.Background(), p)

		var handledValue any
		handler := func(ctx context.Context, _ *fixture.SomethingHappened) error {
			handledValue = ctx.Value(testContextKey{})
			return nil
		}
		require.NoError(t, event.Handle(ctxWithPublisher, handler))

		publishingCtx := context.WithValue(ctxWithPublisher, testContextKey{}, "value")
		require.NoError(t, event.Publish(publishingCtx, &fixture.SomethingHappened{}))
		assert.Equal(t, "value", handledValue)
	})

	t.Run("should give a subscriber of one type of event", func(t *testing.T) {
		ctx := context.Background()
		var handled []*fixture.SomethingHappened
		subscriber := event.HandledBy(func(_ context.Context, e *fixture.SomethingHappened) error {
			handled = append(handled, e)
			return nil
		})

		sh := fixture.NewSomethingHappened()
		require.NoError(t, subscriber.Handle(ctx, fixture.NewSomethingElseHappened()))
		require.NoError(t, subscriber.Handle(ctx, sh))
		assert.Equal(t, []*fixture.SomethingHappened{sh}, handled)
	})
}
