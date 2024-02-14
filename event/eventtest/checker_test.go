package eventtest_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/event/eventtest"
	"github.com/grandper/go-eventide/internal/fixture"
)

// itemsOrdered is a value event with fields that cannot be compared with ==.
type itemsOrdered struct {
	items      []string
	quantities map[string]int
}

func (e itemsOrdered) OccurredOn() time.Time {
	return time.Date(2024, time.June, 12, 1, 10, 20, 0, time.UTC)
}

func TestChecker(t *testing.T) {
	somethingHappened := fixture.NewSomethingHappened()
	somethingElseHappened := fixture.NewSomethingElseHappened()
	ctx := context.Background()

	t.Run("should record events handled directly", func(t *testing.T) {
		checker := eventtest.NewChecker()
		require.NoError(t, checker.Handle(ctx, somethingHappened))
		require.NoError(t, checker.Handle(ctx, somethingElseHappened))

		assert.Equal(t, 2, checker.NumEvents())
		assert.Equal(t, []event.Event{somethingHappened, somethingElseHappened}, checker.Events())
	})

	t.Run("should record events when subscribed to a publisher", func(t *testing.T) {
		checker := eventtest.NewChecker()
		p := event.NewPublisher()
		p.Subscribe(checker)
		publishingCtx := event.ContextWithPublisher(ctx, p)

		require.NoError(t, event.Publish(publishingCtx, somethingHappened))

		assert.True(t, checker.Contains(somethingHappened))
		assert.False(t, checker.Contains(somethingElseHappened))
		assert.True(t, checker.AssertContains(t, somethingHappened))
	})

	t.Run("should fail to subscribe if no publisher is available", func(t *testing.T) {
		checker, err := eventtest.Subscribe(ctx)
		require.ErrorIs(t, err, event.ErrNoPublisher)
		assert.Nil(t, checker)
	})

	t.Run("should record events when subscribed to the publisher of a context", func(t *testing.T) {
		publishingCtx := event.ContextWithPublisher(ctx, event.NewPublisher())

		checker, err := eventtest.Subscribe(publishingCtx)
		require.NoError(t, err)
		assert.Equal(t, 0, checker.NumEvents())

		require.NoError(t, event.Publish(publishingCtx, somethingHappened))
		require.NoError(t, event.Publish(publishingCtx, somethingElseHappened))

		assert.True(t, checker.AssertEvents(t, []event.Event{somethingHappened, somethingElseHappened}))
	})

	t.Run("should record events when registered as a scope subscriber", func(t *testing.T) {
		checker := eventtest.NewChecker()
		event.PrivateScope(func(ctx context.Context) {
			require.NoError(t, event.Publish(ctx, somethingHappened))
			require.NoError(t, event.Publish(ctx, somethingElseHappened))
		}, checker)

		assert.True(t, checker.AssertEvents(t, []event.Event{somethingHappened, somethingElseHappened}))
	})

	t.Run("AssertEvents fails when the events differ", func(t *testing.T) {
		mockT := new(testing.T)
		checker := eventtest.NewChecker()
		require.NoError(t, checker.Handle(ctx, somethingHappened))

		assert.False(t, checker.AssertEvents(mockT, []event.Event{somethingElseHappened}))
	})

	t.Run("AssertContains fails when the event was not recorded", func(t *testing.T) {
		mockT := new(testing.T)
		checker := eventtest.NewChecker()
		require.NoError(t, checker.Handle(ctx, somethingHappened))

		assert.False(t, checker.AssertContains(mockT, somethingElseHappened))
	})

	t.Run("should match pointer events with equal content", func(t *testing.T) {
		checker := eventtest.NewChecker()
		require.NoError(t, checker.Handle(ctx, fixture.NewSomethingHappened()))

		assert.True(t, checker.Contains(fixture.NewSomethingHappened()))
		assert.True(t, checker.AssertEvents(t, []event.Event{fixture.NewSomethingHappened()}))
	})

	t.Run("should compare value events with slice and map fields", func(t *testing.T) {
		newItemsOrdered := func(items ...string) itemsOrdered {
			quantities := make(map[string]int, len(items))
			for _, item := range items {
				quantities[item]++
			}
			return itemsOrdered{items: items, quantities: quantities}
		}
		mockT := new(testing.T)
		checker := eventtest.NewChecker()
		require.NoError(t, checker.Handle(ctx, newItemsOrdered("apple", "pear")))

		assert.True(t, checker.Contains(newItemsOrdered("apple", "pear")))
		assert.False(t, checker.Contains(newItemsOrdered("apple")))
		assert.True(t, checker.AssertContains(t, newItemsOrdered("apple", "pear")))
		assert.False(t, checker.AssertContains(mockT, newItemsOrdered("apple")))
		assert.True(t, checker.AssertEvents(t, []event.Event{newItemsOrdered("apple", "pear")}))
		assert.False(t, checker.AssertEvents(mockT, []event.Event{newItemsOrdered("pear", "apple")}))
	})

	t.Run("should match Protocol Buffers events with equal content, even once serialized", func(t *testing.T) {
		mockT := new(testing.T)
		recorded := fixture.NewSomethingRecorded("a reading")
		// Serializing a message writes to its internal state: it no longer
		// deeply equals a fresh message with the same content.
		_, err := proto.Marshal(recorded)
		require.NoError(t, err)
		checker := eventtest.NewChecker()
		require.NoError(t, checker.Handle(ctx, recorded))

		assert.True(t, checker.Contains(fixture.NewSomethingRecorded("a reading")))
		assert.False(t, checker.Contains(fixture.NewSomethingRecorded("another reading")))
		assert.False(t, checker.Contains(somethingHappened))
		assert.True(t, checker.AssertContains(t, fixture.NewSomethingRecorded("a reading")))
		assert.False(t, checker.AssertContains(mockT, fixture.NewSomethingRecorded("another reading")))
		assert.True(t, checker.AssertEvents(t, []event.Event{fixture.NewSomethingRecorded("a reading")}))
		assert.False(t, checker.AssertEvents(mockT, []event.Event{fixture.NewSomethingRecorded("another reading")}))
	})
}
