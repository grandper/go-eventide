package event_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/internal/fixture"
)

func TestInternal(t *testing.T) {
	ctx := context.Background()
	internalEvent := struct {
		*fixture.SomethingHappened
		event.Internal
	}{SomethingHappened: fixture.NewSomethingHappened()}
	externalEvent := struct {
		*fixture.SomethingHappened
	}{SomethingHappened: fixture.NewSomethingHappened()}

	t.Run("should be identifiable", func(t *testing.T) {
		assert.True(t, event.IsInternal(internalEvent))
		assert.False(t, event.IsInternal(externalEvent))
	})

	t.Run("should filtered out internal events", func(t *testing.T) {
		subscriber := &fixture.Subscriber{}
		filteredSubscriber := event.ExcludeInternal(subscriber)

		require.NoError(t, filteredSubscriber.Handle(ctx, internalEvent))
		assert.Equal(t, 0, subscriber.Count())
	})

	t.Run("should not filter external events", func(t *testing.T) {
		subscriber := &fixture.Subscriber{}
		filteredSubscriber := event.ExcludeInternal(subscriber)

		require.NoError(t, filteredSubscriber.Handle(ctx, externalEvent))
		assert.Equal(t, 1, subscriber.Count())
	})

	t.Run("should hand the context to the decorated subscriber", func(t *testing.T) {
		subscriber := &fixture.Subscriber{}
		filteredSubscriber := event.ExcludeInternal(subscriber)

		handlingCtx := context.WithValue(ctx, testContextKey{}, "value")
		require.NoError(t, filteredSubscriber.Handle(handlingCtx, externalEvent))
		require.Len(t, subscriber.Contexts(), 1)
		assert.Equal(t, "value", subscriber.Contexts()[0].Value(testContextKey{}))
	})
}
