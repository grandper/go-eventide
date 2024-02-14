package inmemory_test

import (
	"context"
	"testing"
	"time"

	"github.com/grandper/go-serializer/serializer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/feed/inmemory"
	"github.com/grandper/go-eventide/internal/fixture"
)

func TestEventStore(t *testing.T) {
	ctx := context.Background()
	// e1 and e3 occurred on the 12th of June 2024, e2 on the 13th.
	e1 := fixture.NewSomethingHappened()
	e2 := fixture.NewSomethingElseHappened()
	e3 := fixture.NewSomethingHappened()

	newStore := func(t *testing.T, events ...event.Event) *inmemory.EventStore {
		t.Helper()
		eventStore := inmemory.NewEventStore(serializer.NewJSONSerializer())
		for _, e := range events {
			require.NoError(t, eventStore.Append(ctx, e))
		}
		return eventStore
	}

	t.Run("should be empty at first", func(t *testing.T) {
		eventStore := newStore(t)

		last, err := eventStore.LastStoredEventID(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), last)

		events, err := eventStore.StoredEventsAfter(ctx, 0, 10)
		require.NoError(t, err)
		assert.Empty(t, events)
	})

	t.Run("can store events and tell the id of the last one", func(t *testing.T) {
		eventStore := newStore(t, e1, e2)

		last, err := eventStore.LastStoredEventID(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(2), last)
	})

	t.Run("can retrieve the events stored after an event", func(t *testing.T) {
		eventStore := newStore(t, e1, e2, e3)

		events, err := eventStore.StoredEventsAfter(ctx, 1, 10)
		require.NoError(t, err)
		require.Len(t, events, 2)
		assert.Equal(t, int64(2), events[0].ID())
		assert.Equal(t, event.Name(e2), events[0].Type())
		assert.Equal(t, e2.OccurredOn().UTC(), events[0].OccurredOn())
		decoder := serializer.NewJSONSerializer(serializer.Register(&fixture.SomethingElseHappened{}))
		decoded, err := decoder.Deserialize(events[0].Body())
		require.NoError(t, err)
		assert.Equal(t, e2, decoded)
		assert.Equal(t, int64(3), events[1].ID())
	})

	t.Run("returns at most the requested number of events", func(t *testing.T) {
		eventStore := newStore(t, e1, e2, e3)

		events, err := eventStore.StoredEventsAfter(ctx, 0, 2)
		require.NoError(t, err)
		require.Len(t, events, 2)
		assert.Equal(t, int64(1), events[0].ID())
		assert.Equal(t, int64(2), events[1].ID())

		events, err = eventStore.StoredEventsAfter(ctx, 0, 0)
		require.NoError(t, err)
		assert.Empty(t, events)
	})

	t.Run("returns no events after the last one", func(t *testing.T) {
		eventStore := newStore(t, e1)

		for _, eventID := range []int64{1, 5} {
			events, err := eventStore.StoredEventsAfter(ctx, eventID, 10)
			require.NoError(t, err)
			assert.Empty(t, events)
		}
	})

	t.Run("can find the first event that occurred since a time", func(t *testing.T) {
		eventStore := newStore(t, e1, e2, e3)

		for name, tc := range map[string]struct {
			since time.Time
			want  int64
		}{
			"before every event":    {e1.OccurredOn().Add(-time.Hour), 1},
			"at the first event":    {e1.OccurredOn(), 1},
			"after the first event": {e1.OccurredOn().Add(time.Second), 2},
			"at the second event":   {e2.OccurredOn(), 2},
			"after every event":     {e2.OccurredOn().Add(time.Second), 0},
		} {
			t.Run(name, func(t *testing.T) {
				first, err := eventStore.FirstStoredEventIDSince(ctx, tc.since)
				require.NoError(t, err)
				assert.Equal(t, tc.want, first)
			})
		}
	})
	// storedIDs returns the ids of every event still in the store.
	storedIDs := func(t *testing.T, eventStore *inmemory.EventStore) []int64 {
		t.Helper()
		events, err := eventStore.StoredEventsAfter(ctx, 0, 100)
		require.NoError(t, err)
		ids := make([]int64, 0, len(events))
		for _, e := range events {
			ids = append(ids, e.ID())
		}
		return ids
	}

	t.Run("can delete every event", func(t *testing.T) {
		eventStore := newStore(t, e1, e2, e3)

		require.NoError(t, eventStore.DeleteAllStoredEvents(ctx))

		assert.Empty(t, storedIDs(t, eventStore))
		last, err := eventStore.LastStoredEventID(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), last)
		first, err := eventStore.FirstStoredEventIDSince(ctx, e1.OccurredOn().Add(-time.Hour))
		require.NoError(t, err)
		assert.Equal(t, int64(0), first)
	})

	t.Run("can delete every event of an empty store", func(t *testing.T) {
		eventStore := newStore(t)

		require.NoError(t, eventStore.DeleteAllStoredEvents(ctx))

		assert.Empty(t, storedIDs(t, eventStore))
	})

	t.Run("can keep only the last events", func(t *testing.T) {
		for name, tc := range map[string]struct {
			count int
			want  []int64
		}{
			"more than stored":  {5, []int64{1, 2, 3}},
			"as many as stored": {3, []int64{1, 2, 3}},
			"fewer than stored": {2, []int64{2, 3}},
			"one":               {1, []int64{3}},
			"none":              {0, []int64{}},
			"negative":          {-1, []int64{}},
		} {
			t.Run(name, func(t *testing.T) {
				eventStore := newStore(t, e1, e2, e3)

				require.NoError(t, eventStore.KeepLastStoredEvents(ctx, tc.count))

				assert.Equal(t, tc.want, storedIDs(t, eventStore))
			})
		}
	})

	t.Run("can delete the events before an id", func(t *testing.T) {
		for name, tc := range map[string]struct {
			eventID int64
			want    []int64
		}{
			"negative":            {-1, []int64{1, 2, 3}},
			"zero":                {0, []int64{1, 2, 3}},
			"the first id":        {1, []int64{1, 2, 3}},
			"an id in the middle": {2, []int64{2, 3}},
			"the last id":         {3, []int64{3}},
			"after the last id":   {4, []int64{}},
			"far after the last":  {10, []int64{}},
		} {
			t.Run(name, func(t *testing.T) {
				eventStore := newStore(t, e1, e2, e3)

				require.NoError(t, eventStore.DeleteStoredEventsBefore(ctx, tc.eventID))

				assert.Equal(t, tc.want, storedIDs(t, eventStore))
			})
		}
	})

	t.Run("can delete the events that occurred before a time", func(t *testing.T) {
		for name, tc := range map[string]struct {
			before time.Time
			want   []int64
		}{
			"before every event":    {e1.OccurredOn().Add(-time.Hour), []int64{1, 2, 3}},
			"at the first event":    {e1.OccurredOn(), []int64{1, 2, 3}},
			"after the first event": {e1.OccurredOn().Add(time.Second), []int64{2}},
			"at the second event":   {e2.OccurredOn(), []int64{2}},
			"after every event":     {e2.OccurredOn().Add(time.Second), []int64{}},
		} {
			t.Run(name, func(t *testing.T) {
				// e1 and e3 occurred on the 12th, e2 on the 13th: e3 is
				// deleted although it is stored after e2.
				eventStore := newStore(t, e1, e2, e3)

				require.NoError(t, eventStore.DeleteStoredEventsOccurredBefore(ctx, tc.before))

				assert.Equal(t, tc.want, storedIDs(t, eventStore))
			})
		}
	})

	t.Run("keeps giving increasing ids after a deletion", func(t *testing.T) {
		for name, del := range map[string]func(t *testing.T, eventStore *inmemory.EventStore){
			"delete all": func(t *testing.T, eventStore *inmemory.EventStore) {
				t.Helper()
				require.NoError(t, eventStore.DeleteAllStoredEvents(ctx))
			},
			"keep last": func(t *testing.T, eventStore *inmemory.EventStore) {
				t.Helper()
				require.NoError(t, eventStore.KeepLastStoredEvents(ctx, 0))
			},
			"delete before id": func(t *testing.T, eventStore *inmemory.EventStore) {
				t.Helper()
				require.NoError(t, eventStore.DeleteStoredEventsBefore(ctx, 4))
			},
			"delete occurred before": func(t *testing.T, eventStore *inmemory.EventStore) {
				t.Helper()
				require.NoError(t, eventStore.DeleteStoredEventsOccurredBefore(ctx, e2.OccurredOn().Add(time.Hour)))
			},
		} {
			t.Run(name, func(t *testing.T) {
				eventStore := newStore(t, e1, e2, e3)
				del(t, eventStore)
				require.NoError(t, eventStore.Append(ctx, e1))

				assert.Equal(t, []int64{4}, storedIDs(t, eventStore))
				last, err := eventStore.LastStoredEventID(ctx)
				require.NoError(t, err)
				assert.Equal(t, int64(4), last)
			})
		}
	})

	t.Run("reads the remaining events after a deletion in the middle", func(t *testing.T) {
		eventStore := newStore(t, e1, e2, e3)
		require.NoError(t, eventStore.DeleteStoredEventsOccurredBefore(ctx, e2.OccurredOn()))

		events, err := eventStore.StoredEventsAfter(ctx, 0, 10)
		require.NoError(t, err)
		require.Len(t, events, 1)
		assert.Equal(t, int64(2), events[0].ID())

		first, err := eventStore.FirstStoredEventIDSince(ctx, e1.OccurredOn())
		require.NoError(t, err)
		assert.Equal(t, int64(2), first)
	})
}
