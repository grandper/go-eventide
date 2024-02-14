package event_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/internal/fixture"
)

func TestChanForwarder(t *testing.T) {
	ctx := context.Background()

	t.Run("should forward the event to the channel", func(t *testing.T) {
		ch := make(chan event.Event, 1)
		forwarder := event.NewChanForwarder(ch)

		somethingHappened := fixture.NewSomethingHappened()
		require.NoError(t, forwarder.Handle(ctx, somethingHappened))
		assert.Equal(t, somethingHappened, <-ch)
	})

	t.Run("should forward the events in the order they are handled", func(t *testing.T) {
		ch := make(chan event.Event, 2)
		forwarder := event.NewChanForwarder(ch)

		somethingHappened := fixture.NewSomethingHappened()
		somethingElseHappened := fixture.NewSomethingElseHappened()
		assert.NoError(t, forwarder.Handle(ctx, somethingHappened))
		assert.NoError(t, forwarder.Handle(ctx, somethingElseHappened))
		assert.Equal(t, somethingHappened, <-ch)
		assert.Equal(t, somethingElseHappened, <-ch)
	})

	t.Run("should unblock a blocked Handle call when the context is cancelled", func(t *testing.T) {
		ch := make(chan event.Event)
		forwarder := event.NewChanForwarder(ch)

		handlingCtx, cancel := context.WithCancel(ctx)
		handleDone := make(chan error)
		go func() {
			handleDone <- forwarder.Handle(handlingCtx, fixture.NewSomethingHappened())
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
}
