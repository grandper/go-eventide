package event_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/internal/fixture"
)

func newContextWithPublisher() context.Context {
	return event.ContextWithPublisher(context.Background(), event.NewPublisher())
}

func TestChanSubscriber(t *testing.T) {
	somethingHappened := fixture.NewSomethingHappened()
	somethingElseHappened := fixture.NewSomethingElseHappened()

	t.Run("should return an error if no publisher is available", func(t *testing.T) {
		_, err := event.NewChanSubscriber(context.Background(), 1)
		assert.ErrorIs(t, err, event.ErrNoPublisher)
	})

	t.Run("should deliver the published event on the channel", func(t *testing.T) {
		ctx := newContextWithPublisher()
		s, err := event.NewChanSubscriber(ctx, 1)
		require.NoError(t, err)
		defer s.Close()

		require.NoError(t, event.Publish(ctx, somethingHappened))
		assert.Equal(t, somethingHappened, <-s.Ch())
	})

	t.Run("should deliver the events in the order they are published", func(t *testing.T) {
		ctx := newContextWithPublisher()
		s, err := event.NewChanSubscriber(ctx, 2)
		require.NoError(t, err)
		defer s.Close()

		require.NoError(t, event.Publish(ctx, somethingHappened))
		require.NoError(t, event.Publish(ctx, somethingElseHappened))
		assert.Equal(t, somethingHappened, <-s.Ch())
		assert.Equal(t, somethingElseHappened, <-s.Ch())
	})

	t.Run("should terminate a range loop when the subscriber is closed", func(t *testing.T) {
		ctx := newContextWithPublisher()
		s, err := event.NewChanSubscriber(ctx, 1)
		require.NoError(t, err)

		rangeDone := make(chan struct{})
		go func() {
			for e := range s.Ch() {
				_ = e // drain the channel until Close ends the range.
			}
			close(rangeDone)
		}()

		s.Close()
		select {
		case <-rangeDone:
		case <-time.After(time.Second):
			assert.Fail(t, "the range loop did not terminate")
		}
	})

	t.Run("should drain buffered events before the range loop terminates", func(t *testing.T) {
		ctx := newContextWithPublisher()
		s, err := event.NewChanSubscriber(ctx, 2)
		require.NoError(t, err)

		require.NoError(t, event.Publish(ctx, somethingHappened))
		require.NoError(t, event.Publish(ctx, somethingElseHappened))
		s.Close()

		received := []event.Event{}
		for e := range s.Ch() {
			received = append(received, e)
		}
		assert.Equal(t, []event.Event{somethingHappened, somethingElseHappened}, received)
	})

	t.Run("should drop the events published after the subscriber is closed", func(t *testing.T) {
		ctx := newContextWithPublisher()
		s, err := event.NewChanSubscriber(ctx, 1)
		require.NoError(t, err)

		s.Close()
		require.NoError(t, event.Publish(ctx, somethingHappened))
		_, open := <-s.Ch()
		assert.False(t, open)
	})

	t.Run("should be safe to close the subscriber twice", func(t *testing.T) {
		ctx := newContextWithPublisher()
		s, err := event.NewChanSubscriber(ctx, 1)
		require.NoError(t, err)

		s.Close()
		s.Close()
	})

	t.Run("should unblock a blocked Handle call when the subscriber is closed", func(t *testing.T) {
		ctx := newContextWithPublisher()
		s, err := event.NewChanSubscriber(ctx, 0)
		require.NoError(t, err)

		handleDone := make(chan error)
		go func() {
			handleDone <- s.Handle(ctx, somethingHappened)
		}()

		time.Sleep(10 * time.Millisecond)
		s.Close()
		select {
		case handleErr := <-handleDone:
			require.NoError(t, handleErr)
		case <-time.After(time.Second):
			assert.Fail(t, "Handle did not unblock")
		}
	})

	t.Run("should unblock a blocked Handle call when the context is cancelled", func(t *testing.T) {
		ctx := newContextWithPublisher()
		s, err := event.NewChanSubscriber(ctx, 0)
		require.NoError(t, err)
		defer s.Close()

		handlingCtx, cancel := context.WithCancel(ctx)
		handleDone := make(chan error)
		go func() {
			handleDone <- s.Handle(handlingCtx, somethingHappened)
		}()

		time.Sleep(10 * time.Millisecond)
		cancel()
		select {
		case handleErr := <-handleDone:
			require.ErrorIs(t, handleErr, context.Canceled)
		case <-time.After(time.Second):
			assert.Fail(t, "Handle did not unblock")
		}
	})

	t.Run("should receive the events published within a scope", func(t *testing.T) {
		event.PrivateScope(func(ctx context.Context) {
			s, err := event.NewChanSubscriber(ctx, 0)
			require.NoError(t, err)

			received := make(chan event.Event, 1)
			go func() {
				received <- <-s.Ch()
			}()

			require.NoError(t, event.Publish(ctx, somethingHappened))
			assert.Equal(t, somethingHappened, <-received)
			s.Close()
		})
	})

	t.Run("should tolerate concurrent Handle and Close calls", func(t *testing.T) {
		ctx := newContextWithPublisher()
		s, err := event.NewChanSubscriber(ctx, 1)
		require.NoError(t, err)

		go func() {
			for e := range s.Ch() {
				_ = e // drain the channel until Close ends the range.
			}
		}()

		var wg sync.WaitGroup
		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 100 {
					assert.NoError(t, s.Handle(ctx, somethingHappened))
				}
			}()
		}
		s.Close()
		wg.Wait()
	})
}
