package event_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/internal/fixture"
)

type testContextKey struct{}

func TestPublisher(t *testing.T) {
	ctx := context.Background()
	sh := &fixture.SomethingHappened{}
	seh := &fixture.SomethingElseHappened{}

	t.Run("should publish events", func(t *testing.T) {
		p := event.NewPublisher()
		s := &fixture.Subscriber{}
		p.Subscribe(s)
		assert.Equal(t, 0, s.Count())
		assert.NoError(t, p.Publish(ctx, sh))
		assert.NoError(t, p.Publish(ctx, seh))
		assert.NoError(t, p.Publish(ctx, sh))
		assert.Equal(t, []event.Event{sh, seh, sh}, s.Handled())

		failingSubscriber := &fixture.Subscriber{Err: errors.New("failed to handle event")}
		p.Subscribe(failingSubscriber)
		assert.Error(t, p.Publish(ctx, sh))
	})

	t.Run("should hand the publishing context to the subscribers", func(t *testing.T) {
		p := event.NewPublisher()
		s := &fixture.Subscriber{}
		p.Subscribe(s)

		publishingCtx := context.WithValue(ctx, testContextKey{}, "value")
		require.NoError(t, p.Publish(publishingCtx, sh))
		require.Len(t, s.Contexts(), 1)
		assert.Equal(t, "value", s.Contexts()[0].Value(testContextKey{}))
	})

	t.Run("should stop delivering the event when the context is done", func(t *testing.T) {
		p := event.NewPublisher()
		s := &fixture.Subscriber{}
		p.Subscribe(s)

		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()
		err := p.Publish(cancelledCtx, sh)
		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 0, s.Count())
	})

	t.Run("should release the lock when a subscriber fails", func(t *testing.T) {
		p := event.NewPublisher()
		p.Subscribe(&fixture.Subscriber{Err: errors.New("failed to handle event")})
		require.Error(t, p.Publish(ctx, sh))

		// Subscribe and Reset take the write lock: they deadlock
		// if the failed Publish did not release its read lock.
		p.Subscribe(&fixture.Subscriber{})
		p.Reset()
		assert.Equal(t, 0, p.SubscriberCount())
	})

	t.Run("should remove the subscribers", func(t *testing.T) {
		p := event.NewPublisher()
		assert.Equal(t, 0, p.SubscriberCount())
		p.Subscribe(&fixture.Subscriber{})
		assert.Equal(t, 1, p.SubscriberCount())
		p.Reset()
		assert.Equal(t, 0, p.SubscriberCount())
	})
}

// TestPublisherReentrancy covers the calls a subscriber makes, from inside
// Handle, to the publisher that is calling it.
func TestPublisherReentrancy(t *testing.T) {
	ctx := context.Background()
	sh := &fixture.SomethingHappened{}
	seh := &fixture.SomethingElseHappened{}

	t.Run("should let a subscriber publish", func(t *testing.T) {
		p := event.NewPublisher()
		p.Subscribe(subscriberFunc(func(ctx context.Context, e event.Event) error {
			if _, ok := e.(*fixture.SomethingHappened); !ok {
				return nil
			}
			return p.Publish(ctx, seh)
		}))
		s := &fixture.Subscriber{}
		p.Subscribe(s)

		requireReturns(t, func() {
			assert.NoError(t, p.Publish(ctx, sh))
		})
		// The nested event is delivered while the original one still is.
		assert.Equal(t, []event.Event{seh, sh}, s.Handled())
	})

	// A nested Publish used to take the read lock its caller was already
	// holding, which deadlocks as soon as a writer is waiting in between.
	// Here the nested Publish only starts once the concurrent call has
	// returned, so the test fails if the delivery holds a lock.
	concurrentCalls := []struct {
		name     string
		call     func(p *event.Publisher)
		expected []event.Event
	}{
		{
			name:     "subscribes",
			call:     func(p *event.Publisher) { p.Subscribe(&fixture.Subscriber{}) },
			expected: []event.Event{seh, sh},
		},
		{
			name: "resets the publisher",
			call: func(p *event.Publisher) { p.Reset() },
			// The nested event is published after the reset.
			expected: []event.Event{sh},
		},
		{
			name:     "counts the subscribers",
			call:     func(p *event.Publisher) { p.SubscriberCount() },
			expected: []event.Event{seh, sh},
		},
	}
	for _, tc := range concurrentCalls {
		t.Run("should let a subscriber publish while another goroutine "+tc.name, func(t *testing.T) {
			p := event.NewPublisher()
			p.Subscribe(subscriberFunc(func(ctx context.Context, e event.Event) error {
				if _, ok := e.(*fixture.SomethingHappened); !ok {
					return nil
				}
				called := make(chan struct{})
				go func() {
					defer close(called)
					tc.call(p)
				}()
				<-called
				return p.Publish(ctx, seh)
			}))
			s := &fixture.Subscriber{}
			p.Subscribe(s)

			requireReturns(t, func() {
				assert.NoError(t, p.Publish(ctx, sh))
			})
			assert.Equal(t, tc.expected, s.Handled())
		})
	}

	t.Run("should let a subscriber subscribe to the publisher", func(t *testing.T) {
		p := event.NewPublisher()
		late := &fixture.Subscriber{}
		var once sync.Once
		p.Subscribe(subscriberFunc(func(context.Context, event.Event) error {
			once.Do(func() { p.Subscribe(late) })
			return nil
		}))
		s := &fixture.Subscriber{}
		p.Subscribe(s)

		requireReturns(t, func() {
			assert.NoError(t, p.Publish(ctx, sh))
		})
		// The event being delivered does not reach the new subscriber, and
		// still reaches the subscribers registered before the delivery.
		assert.Equal(t, 0, late.Count())
		assert.Equal(t, []event.Event{sh}, s.Handled())
		assert.Equal(t, 3, p.SubscriberCount())

		require.NoError(t, p.Publish(ctx, seh))
		assert.Equal(t, []event.Event{seh}, late.Handled())
		assert.Equal(t, []event.Event{sh, seh}, s.Handled())
	})

	t.Run("should let a subscriber subscribe through the context", func(t *testing.T) {
		p := event.NewPublisher()
		ctxWithPublisher := event.ContextWithPublisher(ctx, p)
		late := &fixture.Subscriber{}
		var once sync.Once
		p.Subscribe(subscriberFunc(func(ctx context.Context, _ event.Event) error {
			var err error
			once.Do(func() { err = event.Subscribe(ctx, late) })
			return err
		}))

		requireReturns(t, func() {
			assert.NoError(t, event.Publish(ctxWithPublisher, sh))
		})
		assert.Equal(t, 0, late.Count())

		require.NoError(t, event.Publish(ctxWithPublisher, seh))
		assert.Equal(t, []event.Event{seh}, late.Handled())
	})

	t.Run("should let a subscriber reset the publisher", func(t *testing.T) {
		p := event.NewPublisher()
		p.Subscribe(subscriberFunc(func(context.Context, event.Event) error {
			p.Reset()
			return nil
		}))
		s := &fixture.Subscriber{}
		p.Subscribe(s)

		requireReturns(t, func() {
			assert.NoError(t, p.Publish(ctx, sh))
		})
		// The delivery in progress is not interrupted by the reset.
		assert.Equal(t, []event.Event{sh}, s.Handled())
		assert.Equal(t, 0, p.SubscriberCount())

		require.NoError(t, p.Publish(ctx, seh))
		assert.Equal(t, []event.Event{sh}, s.Handled())
	})

	t.Run("should let a subscriber count the subscribers", func(t *testing.T) {
		p := event.NewPublisher()
		count := -1
		p.Subscribe(subscriberFunc(func(context.Context, event.Event) error {
			count = p.SubscriberCount()
			return nil
		}))

		requireReturns(t, func() {
			assert.NoError(t, p.Publish(ctx, sh))
		})
		assert.Equal(t, 1, count)
	})
}

// TestPublisherDuringDelivery covers the calls made by other goroutines while
// a subscriber is busy handling an event.
func TestPublisherDuringDelivery(t *testing.T) {
	ctx := context.Background()
	sh := &fixture.SomethingHappened{}
	seh := &fixture.SomethingElseHappened{}

	t.Run("should not block the other goroutines", func(t *testing.T) {
		p := event.NewPublisher()
		blocking := newBlockingSubscriber()
		defer blocking.Release()
		p.Subscribe(blocking)
		s := &fixture.Subscriber{}
		p.Subscribe(s)

		published := make(chan error, 1)
		go func() { published <- p.Publish(ctx, sh) }()
		requireReturns(t, blocking.WaitUntilEntered)

		late := &fixture.Subscriber{}
		requireReturns(t, func() { p.Subscribe(late) })
		requireReturns(t, func() {
			assert.Equal(t, 3, p.SubscriberCount())
		})
		// Another event goes through, new subscriber included, while the
		// first one is still being delivered.
		requireReturns(t, func() {
			assert.NoError(t, p.Publish(ctx, seh))
		})
		assert.Equal(t, []event.Event{seh}, s.Handled())
		assert.Equal(t, []event.Event{seh}, late.Handled())

		blocking.Release()
		requireReturns(t, func() {
			assert.NoError(t, <-published)
		})
		// The subscriber added during the delivery misses the event.
		assert.Equal(t, []event.Event{seh, sh}, s.Handled())
		assert.Equal(t, []event.Event{seh}, late.Handled())
	})

	t.Run("should not interrupt the delivery when the publisher is reset", func(t *testing.T) {
		p := event.NewPublisher()
		blocking := newBlockingSubscriber()
		defer blocking.Release()
		p.Subscribe(blocking)
		s := &fixture.Subscriber{}
		p.Subscribe(s)

		published := make(chan error, 1)
		go func() { published <- p.Publish(ctx, sh) }()
		requireReturns(t, blocking.WaitUntilEntered)

		requireReturns(t, p.Reset)
		assert.Equal(t, 0, p.SubscriberCount())

		blocking.Release()
		requireReturns(t, func() {
			assert.NoError(t, <-published)
		})
		assert.Equal(t, []event.Event{sh}, s.Handled())

		require.NoError(t, p.Publish(ctx, seh))
		assert.Equal(t, []event.Event{sh}, s.Handled())
	})

	t.Run("should not block the other goroutines while a chan subscriber is full", func(t *testing.T) {
		p := event.NewPublisher()
		ctxWithPublisher := event.ContextWithPublisher(ctx, p)
		entered := make(chan struct{})
		p.Subscribe(subscriberFunc(func(_ context.Context, e event.Event) error {
			if _, ok := e.(*fixture.SomethingElseHappened); ok {
				close(entered)
			}
			return nil
		}))
		cs, err := event.NewChanSubscriber(ctxWithPublisher, 1)
		require.NoError(t, err)
		defer cs.Close()

		// The first event fills the channel, the second one blocks.
		require.NoError(t, p.Publish(ctx, sh))
		published := make(chan error, 1)
		go func() { published <- p.Publish(ctx, seh) }()
		requireReturns(t, func() { <-entered })

		s := &fixture.Subscriber{}
		requireReturns(t, func() { p.Subscribe(s) })
		requireReturns(t, func() {
			assert.Equal(t, 3, p.SubscriberCount())
		})
		select {
		case publishErr := <-published:
			require.FailNow(t, "the event should not be delivered yet", "error: %v", publishErr)
		default:
		}

		assert.Equal(t, sh, <-cs.Ch())
		assert.Equal(t, seh, <-cs.Ch())
		requireReturns(t, func() {
			assert.NoError(t, <-published)
		})
	})
}

// TestPublisherConcurrency is meant for the race detector.
func TestPublisherConcurrency(t *testing.T) {
	const goroutines = 8
	const iterations = 100

	ctx := context.Background()
	sh := &fixture.SomethingHappened{}
	seh := &fixture.SomethingElseHappened{}

	t.Run("should publish and subscribe concurrently", func(t *testing.T) {
		p := event.NewPublisher()
		s := &fixture.Subscriber{}
		p.Subscribe(s)

		repeatConcurrently(t, goroutines, iterations,
			func() { assert.NoError(t, p.Publish(ctx, sh)) },
			func() { p.Subscribe(&fixture.Subscriber{}) },
		)

		// No subscriber is lost, and none misses an event.
		assert.Equal(t, goroutines*iterations, s.Count())
		assert.Equal(t, 1+goroutines*iterations, p.SubscriberCount())
	})

	t.Run("should support every operation concurrently", func(t *testing.T) {
		p := event.NewPublisher()
		republisher := subscriberFunc(func(ctx context.Context, e event.Event) error {
			if _, ok := e.(*fixture.SomethingHappened); !ok {
				return nil
			}
			return p.Publish(ctx, seh)
		})
		repeatConcurrently(t, goroutines, iterations,
			func() { assert.NoError(t, p.Publish(ctx, sh)) },
			func() { p.Subscribe(republisher) },
			func() { p.Subscribe(&fixture.Subscriber{}) },
			func() { p.SubscriberCount() },
			func() { p.Reset() },
		)
	})
}

// repeatConcurrently runs each operation iterations times in goroutines
// goroutines of its own, and waits for all of them.
func repeatConcurrently(t *testing.T, goroutines, iterations int, operations ...func()) {
	t.Helper()
	var wg sync.WaitGroup
	for range goroutines {
		for _, operation := range operations {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range iterations {
					operation()
				}
			}()
		}
	}
	requireReturns(t, wg.Wait)
}

// blockingSubscriber is a subscriber whose Handle blocks on the events of
// type SomethingHappened until it is released.
type blockingSubscriber struct {
	enteredOnce sync.Once
	releaseOnce sync.Once
	entered     chan struct{}
	released    chan struct{}
}

func newBlockingSubscriber() *blockingSubscriber {
	return &blockingSubscriber{
		entered:  make(chan struct{}),
		released: make(chan struct{}),
	}
}

func (s *blockingSubscriber) Handle(_ context.Context, e event.Event) error {
	if _, ok := e.(*fixture.SomethingHappened); !ok {
		return nil
	}
	s.enteredOnce.Do(func() { close(s.entered) })
	<-s.released
	return nil
}

// WaitUntilEntered blocks until Handle is blocked on an event.
func (s *blockingSubscriber) WaitUntilEntered() {
	<-s.entered
}

// Release unblocks Handle. It is safe to call it multiple times.
func (s *blockingSubscriber) Release() {
	s.releaseOnce.Do(func() { close(s.released) })
}

// subscriberFunc adapts a function to the event.Subscriber interface.
type subscriberFunc func(ctx context.Context, e event.Event) error

func (f subscriberFunc) Handle(ctx context.Context, e event.Event) error {
	return f(ctx, e)
}

// requireReturns fails the test when fn does not return in time, which is how
// a deadlock shows up.
func requireReturns(t *testing.T, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the call did not return: the publisher is deadlocked")
	}
}

func TestContextWithPublisher(t *testing.T) {
	t.Run("new contexts do not contain a publisher", func(t *testing.T) {
		ctx := context.Background()
		assert.Nil(t, event.PublisherFromContext(ctx))
	})

	t.Run("should not change the context when the publisher is nil", func(t *testing.T) {
		ctx := context.Background()
		ctxWithPublisher := event.ContextWithPublisher(ctx, nil)
		assert.Nil(t, event.PublisherFromContext(ctxWithPublisher))
	})

	t.Run("should add a publisher to a context", func(t *testing.T) {
		ctx := context.Background()
		p := event.NewPublisher()
		ctxWithPublisher := event.ContextWithPublisher(ctx, p)
		assert.NotNil(t, event.PublisherFromContext(ctxWithPublisher))
	})
}

func TestPublish(t *testing.T) {
	sh := &fixture.SomethingHappened{}

	t.Run("should do nothing if no publisher is available", func(t *testing.T) {
		ctx := context.Background()
		assert.NoError(t, event.Publish(ctx, sh))
	})

	t.Run("should publish an event", func(t *testing.T) {
		ctx := context.Background()
		p := event.NewPublisher()
		ctxWithPublisher := event.ContextWithPublisher(ctx, p)
		s := &fixture.Subscriber{}
		p.Subscribe(s)
		assert.Equal(t, 0, s.Count())
		require.NoError(t, event.Publish(ctxWithPublisher, sh))
		assert.Equal(t, []event.Event{sh}, s.Handled())
	})

	t.Run("should hand the publishing context to the subscribers", func(t *testing.T) {
		p := event.NewPublisher()
		ctxWithPublisher := event.ContextWithPublisher(context.Background(), p)
		s := &fixture.Subscriber{}
		p.Subscribe(s)

		require.NoError(t, event.Publish(ctxWithPublisher, sh))
		require.Len(t, s.Contexts(), 1)
		// The subscriber can reach the publisher of the current scope.
		assert.Same(t, p, event.PublisherFromContext(s.Contexts()[0]))
	})
}

func TestSubscribe(t *testing.T) {
	t.Run("should return an error if no publisher is available", func(t *testing.T) {
		ctx := context.Background()
		require.ErrorIs(t, event.Subscribe(ctx, &fixture.Subscriber{}), event.ErrNoPublisher)
		require.ErrorIs(t, event.ChanSubscribe(ctx, make(chan event.Event)), event.ErrNoPublisher)
		require.ErrorIs(t, event.Handle(ctx, func(context.Context, event.Event) error {
			return nil
		}), event.ErrNoPublisher)
	})

	t.Run("should subscribe a subscriber", func(t *testing.T) {
		ctx := context.Background()
		p := event.NewPublisher()
		ctxWithPublisher := event.ContextWithPublisher(ctx, p)
		assert.Equal(t, 0, p.SubscriberCount())

		require.NoError(t, event.Subscribe(ctxWithPublisher, &fixture.Subscriber{}))
		assert.Equal(t, 1, p.SubscriberCount())
	})
}
