package feed_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/feed"
	"github.com/grandper/go-eventide/internal/fixture"
)

func TestEventStoringSubscriber(t *testing.T) {
	ctx := context.Background()

	t.Run("should be created from parameters", func(t *testing.T) {
		ess := feed.NewEventStoringSubscriber(&fixture.EventStore{})
		assert.NotNil(t, ess)
	})

	t.Run("should store published events", func(t *testing.T) {
		es := &fixture.EventStore{}
		e := fixture.NewSomethingHappened()
		ess := feed.NewEventStoringSubscriber(es)

		require.NoError(t, ess.Handle(ctx, e))
		assert.Equal(t, []event.Event{e}, es.Appended)
	})

	t.Run("should return an error if the event cannot be stored", func(t *testing.T) {
		es := &fixture.EventStore{AppendErr: errors.New("cannot store the event")}
		e := fixture.NewSomethingHappened()
		ess := feed.NewEventStoringSubscriber(es)

		assert.Error(t, ess.Handle(ctx, e))
	})
}
