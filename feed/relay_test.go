package feed_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
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

func TestRelay(t *testing.T) {
	ctx := context.Background()
	sh := fixture.NewSomethingHappened()
	seh := fixture.NewSomethingElseHappened()

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
	keysOf := func(messages []fixture.PublishedMessage) []string {
		keys := make([]string, 0, len(messages))
		for _, message := range messages {
			keys = append(keys, message.Key)
		}
		return keys
	}

	t.Run("should publish the events of the feed as they are stored", func(t *testing.T) {
		source, store := newFeed(t, sh, seh)
		stored, err := store.StoredEventsAfter(ctx, 0, 2)
		require.NoError(t, err)
		positions := inmemory.NewPositionStore()
		publisher := &fixture.MessagePublisher{}
		relay := feed.NewRelay(source, positions, publisher)

		require.NoError(t, relay.PublishPendingEvents(ctx))
		assert.Equal(t, []fixture.PublishedMessage{
			{Key: "SomethingHappened", Message: stored[0].Body()},
			{Key: "SomethingElseHappened", Message: stored[1].Body()},
		}, publisher.Published())
		requirePosition(t, positions, 2)
	})

	t.Run("should publish what a listener can read", func(t *testing.T) {
		store := inmemory.NewEventStore(serializer.NewJSONSerializer())
		require.NoError(t, store.Append(ctx, sh))
		publisher := &fixture.MessagePublisher{}
		relay := feed.NewRelay(feed.NewStoreFeed(store), inmemory.NewPositionStore(), publisher)
		require.NoError(t, relay.PublishPendingEvents(ctx))
		require.Len(t, publisher.Published(), 1)

		// The receiving service.
		published := publisher.Published()[0]
		messageListener := &fixture.MessageListener{
			Messages: []fixture.IncomingMessage{{Key: published.Key, Body: published.Message}},
		}
		receiving := serializer.NewJSONSerializer(serializer.Register(&fixture.SomethingHappened{}))
		listener := event.NewListener(messageListener, receiving)
		subscriber := &fixture.Subscriber{}

		require.NoError(t, listener.ListenToEvents(ctx, []string{"topic-name"}, subscriber))
		assert.Equal(t, []error{nil}, messageListener.HandlerErrs)
		assert.Equal(t, []event.Event{sh}, subscriber.Handled())
	})

	t.Run("should only publish what was stored since the last run", func(t *testing.T) {
		source, store := newFeed(t, sh, seh)
		positions := inmemory.NewPositionStore()
		publisher := &fixture.MessagePublisher{}
		relay := feed.NewRelay(source, positions, publisher)
		require.NoError(t, relay.PublishPendingEvents(ctx))

		require.NoError(t, relay.PublishPendingEvents(ctx))
		assert.Len(t, publisher.Published(), 2, "nothing is published twice")

		require.NoError(t, store.Append(ctx, sh))
		require.NoError(t, relay.PublishPendingEvents(ctx))
		assert.Len(t, publisher.Published(), 3)
		requirePosition(t, positions, 3)
	})

	t.Run("should resume from the saved position", func(t *testing.T) {
		source, _ := newFeed(t, sh, sh, seh)
		positions := inmemory.NewPositionStore()
		require.NoError(t, positions.SavePosition(ctx, 2))
		publisher := &fixture.MessagePublisher{}
		// The origin of the relay is not used: it has a position.
		relay := feed.NewRelay(source, positions, publisher, feed.StartingFrom(feed.FromEnd()))

		require.NoError(t, relay.PublishPendingEvents(ctx))
		assert.Equal(t, []string{"SomethingElseHappened"}, keysOf(publisher.Published()))
		requirePosition(t, positions, 3)
	})

	t.Run("should begin at the end of the feed the first time", func(t *testing.T) {
		source, store := newFeed(t, sh, sh)
		positions := inmemory.NewPositionStore()
		publisher := &fixture.MessagePublisher{}
		relay := feed.NewRelay(source, positions, publisher, feed.StartingFrom(feed.FromEnd()))

		require.NoError(t, relay.PublishPendingEvents(ctx))
		assert.Empty(t, publisher.Published())
		requirePosition(t, positions, 2)

		require.NoError(t, store.Append(ctx, seh))
		require.NoError(t, relay.PublishPendingEvents(ctx))
		assert.Equal(t, []string{"SomethingElseHappened"}, keysOf(publisher.Published()))
	})

	t.Run("should do nothing when no event is pending", func(t *testing.T) {
		source, _ := newFeed(t)
		positions := inmemory.NewPositionStore()
		publisher := &fixture.MessagePublisher{}
		relay := feed.NewRelay(source, positions, publisher)

		require.NoError(t, relay.PublishPendingEvents(ctx))
		assert.Empty(t, publisher.Published())
		requirePosition(t, positions, 0)
	})

	t.Run("should publish by batches and save the position after each of them", func(t *testing.T) {
		source, _ := newFeed(t, sh, seh, sh, seh, sh)
		counting := &countingFeed{inner: source}
		positions := &recordingPositionStore{inner: inmemory.NewPositionStore()}
		publisher := &fixture.MessagePublisher{}
		relay := feed.NewRelay(counting, positions, publisher, feed.WithBatchLimit(2))

		require.NoError(t, relay.PublishPendingEvents(ctx))
		assert.Len(t, publisher.Published(), 5)
		assert.Equal(t, []feed.Origin{feed.FromStart(), feed.After(2), feed.After(4)}, counting.origins)
		assert.Equal(t, []int{2, 2, 2}, counting.limits)
		assert.Equal(t, []int64{2, 4, 5}, positions.saved)
	})

	t.Run("should read batches of the default limit", func(t *testing.T) {
		source, _ := newFeed(t, sh)
		counting := &countingFeed{inner: source}
		relay := feed.NewRelay(counting, inmemory.NewPositionStore(), &fixture.MessagePublisher{})

		require.NoError(t, relay.PublishPendingEvents(ctx))
		assert.Equal(t, []int{feed.DefaultLimit}, counting.limits)
	})

	t.Run("should stop when an event cannot be published, and publish it again", func(t *testing.T) {
		source, _ := newFeed(t, sh, seh, sh)
		positions := inmemory.NewPositionStore()
		publishErr := errors.New("failed to publish")
		publisher := &failingOnKeyPublisher{key: "SomethingElseHappened", err: publishErr}
		relay := feed.NewRelay(source, positions, publisher)

		err := relay.PublishPendingEvents(ctx)
		require.ErrorIs(t, err, publishErr)
		assert.Contains(t, err.Error(), "failed to publish the event 2 of type 'SomethingElseHappened'")
		assert.Equal(t, []string{"SomethingHappened"}, publisher.keys)
		requirePosition(t, positions, 1)

		publisher.err = nil
		require.NoError(t, relay.PublishPendingEvents(ctx))
		assert.Equal(t, []string{"SomethingHappened", "SomethingElseHappened", "SomethingHappened"}, publisher.keys)
		requirePosition(t, positions, 3)
	})

	t.Run("should fail when the feed cannot be read", func(t *testing.T) {
		readErr := errors.New("failed to read")
		positions := inmemory.NewPositionStore()
		relay := feed.NewRelay(&staticFeed{err: readErr}, positions, &fixture.MessagePublisher{})

		require.ErrorIs(t, relay.PublishPendingEvents(ctx), readErr)
		_, found, err := positions.Position(ctx)
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("should fail when the position cannot be loaded", func(t *testing.T) {
		source, _ := newFeed(t, sh)
		loadErr := errors.New("failed to load")
		publisher := &fixture.MessagePublisher{}
		relay := feed.NewRelay(source, &failingPositionStore{loadErr: loadErr}, publisher)

		require.ErrorIs(t, relay.PublishPendingEvents(ctx), loadErr)
		assert.Empty(t, publisher.Published())
	})

	t.Run("should fail when the position cannot be saved", func(t *testing.T) {
		source, _ := newFeed(t, sh)
		saveErr := errors.New("failed to save")
		relay := feed.NewRelay(source, &failingPositionStore{saveErr: saveErr}, &fixture.MessagePublisher{})

		require.ErrorIs(t, relay.PublishPendingEvents(ctx), saveErr)
	})
}

func TestRelayFollow(t *testing.T) {
	const interval = 5 * time.Millisecond
	const waitFor, tick = 2 * time.Second, interval
	ctx := context.Background()
	sh := fixture.NewSomethingHappened()

	newRelay := func(t *testing.T) (*feed.Relay, *inmemory.EventStore, *fixture.MessagePublisher) {
		t.Helper()
		store := inmemory.NewEventStore(serializer.NewJSONSerializer())
		require.NoError(t, store.Append(ctx, sh))
		publisher := &fixture.MessagePublisher{}
		return feed.NewRelay(feed.NewStoreFeed(store), inmemory.NewPositionStore(), publisher), store, publisher
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

	t.Run("should publish the pending events on every interval until stopped", func(t *testing.T) {
		relay, store, publisher := newRelay(t)

		stop, err := relay.Follow(ctx, interval)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return len(publisher.Published()) == 1 }, waitFor, tick)
		require.NoError(t, store.Append(ctx, sh))
		require.Eventually(t, func() bool { return len(publisher.Published()) == 2 }, waitFor, tick)
		stopWithin(t, stop, waitFor)

		require.NoError(t, store.Append(ctx, sh))
		time.Sleep(10 * interval)
		assert.Len(t, publisher.Published(), 2, "no event is published after stop returns")
	})

	t.Run("should stop when the context is done", func(t *testing.T) {
		relay, store, publisher := newRelay(t)
		followingCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		stop, err := relay.Follow(followingCtx, interval)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return len(publisher.Published()) == 1 }, waitFor, tick)
		cancel()
		stopWithin(t, stop, waitFor)

		require.NoError(t, store.Append(ctx, sh))
		time.Sleep(10 * interval)
		assert.Len(t, publisher.Published(), 1, "no event is published once the context is done")
	})

	t.Run("should log a failed run and retry on the next interval", func(t *testing.T) {
		logs := &syncBuffer{}
		logger := slog.New(slog.NewTextHandler(logs, nil))
		relay := feed.NewRelay(
			&staticFeed{err: errors.New("failed to read")}, inmemory.NewPositionStore(), &fixture.MessagePublisher{},
			feed.WithLogger(logger),
		)

		stop, err := relay.Follow(ctx, interval)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return strings.Count(logs.String(), "failed to relay the feed") >= 2
		}, waitFor, tick)
		stopWithin(t, stop, waitFor)
	})

	t.Run("should refuse an interval that is not positive", func(t *testing.T) {
		relay, _, publisher := newRelay(t)

		for _, invalid := range []time.Duration{0, -interval} {
			stop, err := relay.Follow(ctx, invalid)

			require.ErrorIs(t, err, feed.ErrInvalidInterval)
			stopWithin(t, stop, waitFor)
		}
		time.Sleep(10 * interval)
		assert.Empty(t, publisher.Published(), "nothing is followed")
	})

	t.Run("stop can be called more than once", func(t *testing.T) {
		relay, _, _ := newRelay(t)

		stop, err := relay.Follow(ctx, interval)
		require.NoError(t, err)
		stopWithin(t, stop, waitFor)
		stopWithin(t, stop, waitFor)
	})
}

// recordingPositionStore records the positions saved to a position store.
type recordingPositionStore struct {
	inner feed.PositionStore
	saved []int64
}

func (s *recordingPositionStore) Position(ctx context.Context) (int64, bool, error) {
	return s.inner.Position(ctx)
}

func (s *recordingPositionStore) SavePosition(ctx context.Context, position int64) error {
	s.saved = append(s.saved, position)
	return s.inner.SavePosition(ctx, position)
}

// failingOnKeyPublisher is a message publisher that fails, as long as err is
// set, to publish the messages of the given key. It records the keys of the
// messages it published.
type failingOnKeyPublisher struct {
	key  string
	err  error
	keys []string
}

func (p *failingOnKeyPublisher) Publish(_ context.Context, key string, _ []byte) error {
	if key == p.key && p.err != nil {
		return p.err
	}
	p.keys = append(p.keys, key)
	return nil
}
