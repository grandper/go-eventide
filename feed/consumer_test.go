package feed_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grandper/go-serializer/serializer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/feed"
	"github.com/grandper/go-eventide/feed/inmemory"
	"github.com/grandper/go-eventide/internal/fixture"
)

func TestConsumer(t *testing.T) {
	ctx := context.Background()
	// sh occurred on the 12th of June 2024, seh on the 13th.
	sh := fixture.NewSomethingHappened()
	seh := fixture.NewSomethingElseHappened()
	bothTypes := serializer.NewJSONSerializer(
		serializer.Register(&fixture.SomethingHappened{}),
		serializer.Register(&fixture.SomethingElseHappened{}),
	)

	// newFeed returns a feed of the given events, and its store.
	newFeed := func(t *testing.T, events ...event.Event) (*feed.StoreFeed, *inmemory.EventStore) {
		t.Helper()
		store := inmemory.NewEventStore(serializer.NewJSONSerializer())
		for _, e := range events {
			require.NoError(t, store.Append(ctx, e))
		}
		return feed.NewStoreFeed(store), store
	}
	requirePosition := func(t *testing.T, positions feed.PositionStore, want int64) {
		t.Helper()
		position, found, err := positions.Position(ctx)
		require.NoError(t, err)
		require.True(t, found, "a position is saved")
		assert.Equal(t, want, position)
	}

	t.Run("should hand the events of the feed to its subscribers", func(t *testing.T) {
		source, _ := newFeed(t, sh, seh, sh)
		positions := inmemory.NewPositionStore()
		first, second := &fixture.Subscriber{}, &fixture.Subscriber{}
		consumer := feed.NewConsumer(source, bothTypes, positions)
		consumer.Subscribe(first)
		consumer.Subscribe(second)

		require.NoError(t, consumer.CatchUp(ctx))
		assert.Equal(t, []event.Event{sh, seh, sh}, first.Handled())
		assert.Equal(t, []event.Event{sh, seh, sh}, second.Handled())
		requirePosition(t, positions, 3)
	})

	t.Run("should hand the events with the context of the run", func(t *testing.T) {
		source, _ := newFeed(t, sh)
		subscriber := &fixture.Subscriber{}
		consumer := feed.NewConsumer(source, bothTypes, inmemory.NewPositionStore())
		consumer.Subscribe(subscriber)

		type contextKey struct{}
		require.NoError(t, consumer.CatchUp(context.WithValue(ctx, contextKey{}, "value")))
		require.Len(t, subscriber.Contexts(), 1)
		assert.Equal(t, "value", subscriber.Contexts()[0].Value(contextKey{}))
	})

	t.Run("should only hand what happened since the last run", func(t *testing.T) {
		source, store := newFeed(t, sh, seh)
		positions := inmemory.NewPositionStore()
		subscriber := &fixture.Subscriber{}
		consumer := feed.NewConsumer(source, bothTypes, positions)
		consumer.Subscribe(subscriber)
		require.NoError(t, consumer.CatchUp(ctx))

		require.NoError(t, consumer.CatchUp(ctx))
		assert.Equal(t, 2, subscriber.Count(), "nothing new")

		require.NoError(t, store.Append(ctx, sh))
		require.NoError(t, consumer.CatchUp(ctx))
		assert.Equal(t, []event.Event{sh, seh, sh}, subscriber.Handled())
		requirePosition(t, positions, 3)
	})

	t.Run("should resume from the saved position", func(t *testing.T) {
		source, _ := newFeed(t, sh, seh, sh)
		positions := inmemory.NewPositionStore()
		require.NoError(t, positions.SavePosition(ctx, 2))
		subscriber := &fixture.Subscriber{}
		// The origin of the consumer is not used: it has a position.
		consumer := feed.NewConsumer(source, bothTypes, positions, feed.StartingFrom(feed.FromEnd()))
		consumer.Subscribe(subscriber)

		require.NoError(t, consumer.CatchUp(ctx))
		assert.Equal(t, []event.Event{sh}, subscriber.Handled())
		requirePosition(t, positions, 3)
	})

	t.Run("should read batch after batch until the end", func(t *testing.T) {
		source, _ := newFeed(t, sh, seh, sh, seh, sh)
		counting := &countingFeed{inner: source}
		subscriber := &fixture.Subscriber{}
		consumer := feed.NewConsumer(counting, bothTypes, inmemory.NewPositionStore(), feed.WithBatchLimit(2))
		consumer.Subscribe(subscriber)

		require.NoError(t, consumer.CatchUp(ctx))
		assert.Equal(t, []event.Event{sh, seh, sh, seh, sh}, subscriber.Handled())
		assert.Equal(t, []feed.Origin{feed.FromStart(), feed.After(2), feed.After(4)}, counting.origins)
		assert.Equal(t, []int{2, 2, 2}, counting.limits)
	})

	t.Run("should begin at the end of the feed the first time", func(t *testing.T) {
		source, store := newFeed(t, sh, seh)
		positions := inmemory.NewPositionStore()
		subscriber := &fixture.Subscriber{}
		consumer := feed.NewConsumer(source, bothTypes, positions, feed.StartingFrom(feed.FromEnd()))
		consumer.Subscribe(subscriber)

		require.NoError(t, consumer.CatchUp(ctx))
		assert.Empty(t, subscriber.Handled())
		requirePosition(t, positions, 2)

		require.NoError(t, store.Append(ctx, sh))
		require.NoError(t, consumer.CatchUp(ctx))
		assert.Equal(t, []event.Event{sh}, subscriber.Handled())
		requirePosition(t, positions, 3)
	})

	t.Run("should save the start of an empty feed as its position", func(t *testing.T) {
		source, _ := newFeed(t)
		positions := inmemory.NewPositionStore()
		consumer := feed.NewConsumer(source, bothTypes, positions, feed.StartingFrom(feed.FromEnd()))

		require.NoError(t, consumer.CatchUp(ctx))
		requirePosition(t, positions, 0)
	})

	t.Run("should begin at a time the first time", func(t *testing.T) {
		source, _ := newFeed(t, sh, seh, sh)
		positions := inmemory.NewPositionStore()
		subscriber := &fixture.Subscriber{}
		consumer := feed.NewConsumer(source, bothTypes, positions,
			feed.StartingFrom(feed.Since(seh.OccurredOn())))
		consumer.Subscribe(subscriber)

		require.NoError(t, consumer.CatchUp(ctx))
		// The third event occurred before the time, but was stored after the
		// first event that occurred at the time.
		assert.Equal(t, []event.Event{seh, sh}, subscriber.Handled())
		requirePosition(t, positions, 3)
	})

	t.Run("should skip the events whose type is not registered", func(t *testing.T) {
		source, _ := newFeed(t, seh, sh, seh)
		positions := inmemory.NewPositionStore()
		subscriber := &fixture.Subscriber{}
		onlySomethingHappened := serializer.NewJSONSerializer(serializer.Register(&fixture.SomethingHappened{}))
		consumer := feed.NewConsumer(source, onlySomethingHappened, positions)
		consumer.Subscribe(subscriber)

		require.NoError(t, consumer.CatchUp(ctx))
		assert.Equal(t, []event.Event{sh}, subscriber.Handled())
		requirePosition(t, positions, 3)
	})

	t.Run("should hand the events of one type to a handling function", func(t *testing.T) {
		source, _ := newFeed(t, seh, sh, seh)
		var handled []*fixture.SomethingElseHappened
		consumer := feed.NewConsumer(source, bothTypes, inmemory.NewPositionStore())
		consumer.Subscribe(event.HandledBy(func(_ context.Context, e *fixture.SomethingElseHappened) error {
			handled = append(handled, e)
			return nil
		}))

		require.NoError(t, consumer.CatchUp(ctx))
		assert.Equal(t, []*fixture.SomethingElseHappened{seh, seh}, handled)
	})

	t.Run("should stop on an event that cannot be deserialized", func(t *testing.T) {
		_, store := newFeed(t, sh)
		valid, err := store.StoredEventsAfter(ctx, 0, 1)
		require.NoError(t, err)
		corrupted := &staticFeed{events: []*feed.StoredEvent{
			valid[0],
			feed.NewStoredEvent(2, "SomethingHappened", time.Now(), []byte("corrupted")),
			feed.NewStoredEvent(3, "SomethingHappened", time.Now(), valid[0].Body()),
		}}
		positions := inmemory.NewPositionStore()
		subscriber := &fixture.Subscriber{}
		consumer := feed.NewConsumer(corrupted, bothTypes, positions)
		consumer.Subscribe(subscriber)

		err = consumer.CatchUp(ctx)
		require.ErrorIs(t, err, serializer.ErrFailedToDeserialize)
		assert.Equal(t, []event.Event{sh}, subscriber.Handled())
		requirePosition(t, positions, 1)
	})

	t.Run("should stop on a body that is not an event", func(t *testing.T) {
		source, _ := newFeed(t, sh)
		positions := inmemory.NewPositionStore()
		notAnEvent := &fixture.Serializer{Deserialized: "not an event"}
		consumer := feed.NewConsumer(source, notAnEvent, positions)

		err := consumer.CatchUp(ctx)
		require.ErrorIs(t, err, event.ErrNotAnEvent)
		assert.Contains(t, err.Error(), "failed to handle the event 1 of type 'SomethingHappened'")
		requirePosition(t, positions, 0)
	})

	t.Run("should skip the events a serializer of another kind does not know", func(t *testing.T) {
		source, _ := newFeed(t, sh, seh)
		positions := inmemory.NewPositionStore()
		subscriber := &fixture.Subscriber{}
		unknown := &fixture.Serializer{DeserializeErr: fmt.Errorf("my codec: %w", event.ErrTypeNotRegistered)}
		consumer := feed.NewConsumer(source, unknown, positions)
		consumer.Subscribe(subscriber)

		require.NoError(t, consumer.CatchUp(ctx))
		assert.Empty(t, subscriber.Handled())
		requirePosition(t, positions, 2)
	})

	t.Run("should stop when a subscriber fails, and hand the event again", func(t *testing.T) {
		source, _ := newFeed(t, sh, seh, sh)
		positions := inmemory.NewPositionStore()
		handlingErr := errors.New("failed to handle")
		failing := true
		var handled []event.Event
		first := subscriberFunc(func(_ context.Context, e event.Event) error {
			if _, ok := e.(*fixture.SomethingElseHappened); ok && failing {
				return handlingErr
			}
			handled = append(handled, e)
			return nil
		})
		second := &fixture.Subscriber{}
		consumer := feed.NewConsumer(source, bothTypes, positions)
		consumer.Subscribe(first)
		consumer.Subscribe(second)

		require.ErrorIs(t, consumer.CatchUp(ctx), handlingErr)
		assert.Equal(t, []event.Event{sh}, handled)
		assert.Equal(t, []event.Event{sh}, second.Handled(), "the next subscribers do not receive the event")
		requirePosition(t, positions, 1)

		failing = false
		require.NoError(t, consumer.CatchUp(ctx))
		assert.Equal(t, []event.Event{sh, seh, sh}, handled)
		requirePosition(t, positions, 3)
	})

	t.Run("should keep the position before the first event when it fails", func(t *testing.T) {
		source, _ := newFeed(t, sh, seh, sh)
		positions := inmemory.NewPositionStore()
		consumer := feed.NewConsumer(source, bothTypes, positions,
			feed.StartingFrom(feed.Since(seh.OccurredOn())))
		consumer.Subscribe(&fixture.Subscriber{Err: errors.New("failed to handle")})

		require.Error(t, consumer.CatchUp(ctx))
		requirePosition(t, positions, 1)
	})

	t.Run("should fail when the feed cannot be read", func(t *testing.T) {
		readErr := errors.New("failed to read")
		positions := inmemory.NewPositionStore()
		consumer := feed.NewConsumer(&staticFeed{err: readErr}, bothTypes, positions)

		require.ErrorIs(t, consumer.CatchUp(ctx), readErr)
		_, found, err := positions.Position(ctx)
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("should fail when the position cannot be loaded", func(t *testing.T) {
		source, _ := newFeed(t, sh)
		loadErr := errors.New("failed to load")
		subscriber := &fixture.Subscriber{}
		consumer := feed.NewConsumer(source, bothTypes, &failingPositionStore{loadErr: loadErr})
		consumer.Subscribe(subscriber)

		require.ErrorIs(t, consumer.CatchUp(ctx), loadErr)
		assert.Empty(t, subscriber.Handled())
	})

	t.Run("should fail when the position cannot be saved", func(t *testing.T) {
		source, _ := newFeed(t, sh)
		saveErr := errors.New("failed to save")
		consumer := feed.NewConsumer(source, bothTypes, &failingPositionStore{saveErr: saveErr})

		require.ErrorIs(t, consumer.CatchUp(ctx), saveErr)
	})

	t.Run("should report both the failure of a subscriber and the one of the position", func(t *testing.T) {
		source, _ := newFeed(t, sh)
		handlingErr, saveErr := errors.New("failed to handle"), errors.New("failed to save")
		consumer := feed.NewConsumer(source, bothTypes, &failingPositionStore{saveErr: saveErr})
		consumer.Subscribe(&fixture.Subscriber{Err: handlingErr})

		err := consumer.CatchUp(ctx)
		require.ErrorIs(t, err, handlingErr)
		require.ErrorIs(t, err, saveErr)
	})
}

func TestConsumerFollow(t *testing.T) {
	const interval = 5 * time.Millisecond
	const waitFor, tick = 2 * time.Second, interval
	ctx := context.Background()
	sh := fixture.NewSomethingHappened()
	s := serializer.NewJSONSerializer(serializer.Register(&fixture.SomethingHappened{}))

	newFollower := func(
		t *testing.T,
		options ...feed.Option,
	) (*feed.Consumer, *inmemory.EventStore, *fixture.Subscriber) {
		t.Helper()
		store := inmemory.NewEventStore(serializer.NewJSONSerializer())
		require.NoError(t, store.Append(ctx, sh))
		source := feed.NewStoreFeed(store)
		subscriber := &fixture.Subscriber{}
		consumer := feed.NewConsumer(source, s, inmemory.NewPositionStore(), options...)
		consumer.Subscribe(subscriber)
		return consumer, store, subscriber
	}
	stopWithin := func(t *testing.T, stop func(), timeout time.Duration) {
		t.Helper()
		returned := make(chan struct{})
		go func() {
			stop()
			close(returned)
		}()
		select {
		case <-returned:
		case <-time.After(timeout):
			require.Fail(t, "stop did not return in time")
		}
	}

	t.Run("should catch up on every interval until stopped", func(t *testing.T) {
		consumer, store, subscriber := newFollower(t)

		stop, err := consumer.Follow(ctx, interval)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return subscriber.Count() == 1 }, waitFor, tick)
		require.NoError(t, store.Append(ctx, sh))
		require.Eventually(t, func() bool { return subscriber.Count() == 2 }, waitFor, tick)
		stopWithin(t, stop, waitFor)

		require.NoError(t, store.Append(ctx, sh))
		time.Sleep(10 * interval)
		assert.Equal(t, 2, subscriber.Count(), "no event is handed after stop returns")
	})

	t.Run("should stop following when the context is done", func(t *testing.T) {
		consumer, store, subscriber := newFollower(t)
		followingCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		stop, err := consumer.Follow(followingCtx, interval)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return subscriber.Count() == 1 }, waitFor, tick)
		cancel()
		stopWithin(t, stop, waitFor)

		require.NoError(t, store.Append(ctx, sh))
		time.Sleep(10 * interval)
		assert.Equal(t, 1, subscriber.Count(), "no event is handed once the context is done")
	})

	t.Run("should log a failed run and retry on the next interval", func(t *testing.T) {
		logs := &syncBuffer{}
		logger := slog.New(slog.NewTextHandler(logs, nil))
		consumer := feed.NewConsumer(
			&staticFeed{err: errors.New("failed to read")}, s, inmemory.NewPositionStore(),
			feed.WithLogger(logger),
		)

		stop, err := consumer.Follow(ctx, interval)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return strings.Count(logs.String(), "failed to follow the feed") >= 2
		}, waitFor, tick)
		stopWithin(t, stop, waitFor)
	})

	t.Run("should refuse an interval that is not positive", func(t *testing.T) {
		consumer, _, subscriber := newFollower(t)

		for _, invalid := range []time.Duration{0, -interval} {
			stop, err := consumer.Follow(ctx, invalid)

			require.ErrorIs(t, err, feed.ErrInvalidInterval)
			stopWithin(t, stop, waitFor)
		}
		time.Sleep(10 * interval)
		assert.Equal(t, 0, subscriber.Count(), "nothing is followed")
	})

	t.Run("stop can be called more than once", func(t *testing.T) {
		consumer, _, _ := newFollower(t)

		stop, err := consumer.Follow(ctx, interval)
		require.NoError(t, err)
		stopWithin(t, stop, waitFor)
		stopWithin(t, stop, waitFor)
	})

	t.Run("should support catching up and subscribing while following", func(t *testing.T) {
		consumer, store, subscriber := newFollower(t)

		stop, err := consumer.Follow(ctx, interval)
		require.NoError(t, err)
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 20 {
					assert.NoError(t, store.Append(ctx, sh))
					assert.NoError(t, consumer.CatchUp(ctx))
					consumer.Subscribe(&fixture.Subscriber{})
				}
			}()
		}
		wg.Wait()
		stopWithin(t, stop, waitFor)

		assert.Equal(t, 81, subscriber.Count(), "every event is handed once")
	})
}

// subscriberFunc is a function used as a subscriber.
type subscriberFunc func(ctx context.Context, e event.Event) error

func (f subscriberFunc) Handle(ctx context.Context, e event.Event) error {
	return f(ctx, e)
}

// countingFeed records the readings of a feed.
type countingFeed struct {
	inner   feed.Feed
	origins []feed.Origin
	limits  []int
}

func (f *countingFeed) RetrieveEvents(ctx context.Context, origin feed.Origin, limit int) (*feed.Batch, error) {
	f.origins = append(f.origins, origin)
	f.limits = append(f.limits, limit)
	return f.inner.RetrieveEvents(ctx, origin, limit)
}

// staticFeed is a feed of preset events, read from its start whatever the
// origin is, or a feed that fails when err is set.
type staticFeed struct {
	events []*feed.StoredEvent
	err    error
}

func (f *staticFeed) RetrieveEvents(context.Context, feed.Origin, int) (*feed.Batch, error) {
	if f.err != nil {
		return nil, f.err
	}
	var next int64
	if len(f.events) > 0 {
		next = f.events[len(f.events)-1].ID()
	}
	return feed.NewBatch(f.events, next, false), nil
}

// failingPositionStore is a position store that holds no position and fails
// with the errors that are set.
type failingPositionStore struct {
	loadErr error
	saveErr error
}

func (s *failingPositionStore) Position(context.Context) (int64, bool, error) {
	return 0, false, s.loadErr
}

func (s *failingPositionStore) SavePosition(context.Context, int64) error {
	return s.saveErr
}

// syncBuffer is a buffer that can be written by a goroutine while the test
// reads it.
type syncBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}
