package feed_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/feed"
	"github.com/grandper/go-eventide/internal/fixture"
)

func TestStoreFeed(t *testing.T) {
	ctx := context.Background()

	t.Run("should read the feed from its start", func(t *testing.T) {
		store := &fixture.EventStore{After: createTestingEvents(1, 45)}
		storeFeed := feed.NewStoreFeed(store)

		batch, err := storeFeed.RetrieveEvents(ctx, feed.FromStart(), 20)
		require.NoError(t, err)
		assert.Equal(t, eventIDs(1, 20), idsOf(batch.Events()))
		assert.Equal(t, int64(20), batch.Next())
		assert.True(t, batch.HasMore())
		// One more event is read to know whether the feed goes on.
		assert.Equal(t, []fixture.AfterCall{{EventID: 0, Limit: 21}}, store.AfterCalls)
	})

	t.Run("should read the feed after a position", func(t *testing.T) {
		store := &fixture.EventStore{After: createTestingEvents(41, 45)}
		storeFeed := feed.NewStoreFeed(store)

		batch, err := storeFeed.RetrieveEvents(ctx, feed.After(40), 20)
		require.NoError(t, err)
		assert.Equal(t, eventIDs(41, 45), idsOf(batch.Events()))
		assert.Equal(t, int64(45), batch.Next())
		assert.False(t, batch.HasMore())
		assert.Equal(t, []fixture.AfterCall{{EventID: 40, Limit: 21}}, store.AfterCalls)
	})

	t.Run("should not announce more events when the batch is exactly full", func(t *testing.T) {
		store := &fixture.EventStore{After: createTestingEvents(1, 20)}
		storeFeed := feed.NewStoreFeed(store)

		batch, err := storeFeed.RetrieveEvents(ctx, feed.FromStart(), 20)
		require.NoError(t, err)
		assert.Len(t, batch.Events(), 20)
		assert.False(t, batch.HasMore())
	})

	t.Run("should return the position it began at when there is nothing to read", func(t *testing.T) {
		storeFeed := feed.NewStoreFeed(&fixture.EventStore{})

		batch, err := storeFeed.RetrieveEvents(ctx, feed.After(45), 20)
		require.NoError(t, err)
		assert.NotNil(t, batch.Events())
		assert.Empty(t, batch.Events())
		assert.Equal(t, int64(45), batch.Next())
		assert.False(t, batch.HasMore())
	})

	t.Run("should read the feed from its end", func(t *testing.T) {
		store := &fixture.EventStore{LastID: 45}
		storeFeed := feed.NewStoreFeed(store)

		batch, err := storeFeed.RetrieveEvents(ctx, feed.FromEnd(), 20)
		require.NoError(t, err)
		assert.Empty(t, batch.Events())
		assert.Equal(t, int64(45), batch.Next())
		assert.Equal(t, []fixture.AfterCall{{EventID: 45, Limit: 21}}, store.AfterCalls)
	})

	t.Run("should read the feed since a time", func(t *testing.T) {
		since := time.Date(2026, time.September, 26, 0, 0, 0, 0, time.UTC)
		store := &fixture.EventStore{LastID: 45, FirstIDSince: 41, After: createTestingEvents(41, 45)}
		storeFeed := feed.NewStoreFeed(store)

		batch, err := storeFeed.RetrieveEvents(ctx, feed.Since(since), 20)
		require.NoError(t, err)
		assert.Equal(t, eventIDs(41, 45), idsOf(batch.Events()))
		assert.Equal(t, []time.Time{since}, store.FirstIDSinceCalls)
		assert.Equal(t, []fixture.AfterCall{{EventID: 40, Limit: 21}}, store.AfterCalls)
	})

	t.Run("should read the feed from its end when no event occurred since the time", func(t *testing.T) {
		store := &fixture.EventStore{LastID: 45}
		storeFeed := feed.NewStoreFeed(store)

		batch, err := storeFeed.RetrieveEvents(ctx, feed.Since(time.Now()), 20)
		require.NoError(t, err)
		assert.Empty(t, batch.Events())
		assert.Equal(t, int64(45), batch.Next())
	})

	t.Run("should use the default limit when the limit is 0", func(t *testing.T) {
		store := &fixture.EventStore{After: createTestingEvents(1, 45)}
		storeFeed := feed.NewStoreFeed(store)

		batch, err := storeFeed.RetrieveEvents(ctx, feed.FromStart(), 0)
		require.NoError(t, err)
		assert.Len(t, batch.Events(), feed.DefaultLimit)
	})

	t.Run("should accept the highest limit", func(t *testing.T) {
		store := &fixture.EventStore{}
		storeFeed := feed.NewStoreFeed(store)

		_, err := storeFeed.RetrieveEvents(ctx, feed.FromStart(), feed.MaxLimit)
		require.NoError(t, err)
		assert.Equal(t, []fixture.AfterCall{{EventID: 0, Limit: feed.MaxLimit + 1}}, store.AfterCalls)
	})

	t.Run("should fail if the limit is invalid", func(t *testing.T) {
		storeFeed := feed.NewStoreFeed(&fixture.EventStore{})

		for _, limit := range []int{-1, feed.MaxLimit + 1} {
			batch, err := storeFeed.RetrieveEvents(ctx, feed.FromStart(), limit)
			require.ErrorIs(t, err, feed.ErrInvalidLimit)
			assert.Nil(t, batch)
		}
	})

	t.Run("should fail if the position is negative", func(t *testing.T) {
		storeFeed := feed.NewStoreFeed(&fixture.EventStore{})

		batch, err := storeFeed.RetrieveEvents(ctx, feed.After(-1), 20)
		require.ErrorIs(t, err, feed.ErrInvalidPosition)
		assert.Nil(t, batch)
	})

	t.Run("should fail if the store fails", func(t *testing.T) {
		storeErr := errors.New("an error occurred")
		for name, tc := range map[string]struct {
			store  *fixture.EventStore
			origin feed.Origin
		}{
			"to read the events":                {&fixture.EventStore{AfterErr: storeErr}, feed.FromStart()},
			"to find the end":                   {&fixture.EventStore{LastIDErr: storeErr}, feed.FromEnd()},
			"to find the end before a time":     {&fixture.EventStore{LastIDErr: storeErr}, feed.Since(time.Now())},
			"to find the first event at a time": {&fixture.EventStore{FirstIDSinceErr: storeErr}, feed.Since(time.Now())},
		} {
			t.Run(name, func(t *testing.T) {
				storeFeed := feed.NewStoreFeed(tc.store)

				batch, err := storeFeed.RetrieveEvents(ctx, tc.origin, 20)
				require.ErrorIs(t, err, storeErr)
				assert.Nil(t, batch)
			})
		}
	})

	t.Run("should read a store whose ids have gaps", func(t *testing.T) {
		events := append(createTestingEvents(1, 3), createTestingEvents(7, 9)...)
		store := &fixture.EventStore{After: events}
		storeFeed := feed.NewStoreFeed(store)

		batch, err := storeFeed.RetrieveEvents(ctx, feed.FromStart(), 20)
		require.NoError(t, err)
		assert.Equal(t, []int64{1, 2, 3, 7, 8, 9}, idsOf(batch.Events()))
		assert.Equal(t, int64(9), batch.Next())
	})
}

// createTestingEvents returns stored events whose ids go from first to last.
func createTestingEvents(first, last int64) []*feed.StoredEvent {
	eventBody := []byte(`{"occurredOn":"2024-06-12T01:10:20Z"}`)
	events := make([]*feed.StoredEvent, 0, last-first+1)
	for id := first; id <= last; id++ {
		events = append(events, feed.NewStoredEvent(id, "SomethingHappened", time.Now(), eventBody))
	}
	return events
}

// eventIDs returns the ids from first to last.
func eventIDs(first, last int64) []int64 {
	ids := make([]int64, 0, last-first+1)
	for id := first; id <= last; id++ {
		ids = append(ids, id)
	}
	return ids
}

func idsOf(events []*feed.StoredEvent) []int64 {
	ids := make([]int64, 0, len(events))
	for _, e := range events {
		ids = append(ids, e.ID())
	}
	return ids
}
