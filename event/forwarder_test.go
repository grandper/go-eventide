package event_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/internal/fixture"
)

func TestForwarder(t *testing.T) {
	ctx := context.Background()
	testEvent := fixture.NewSomethingHappened()
	serializedEvent := []byte(`{"occurredOn":"2024-06-12T01:10:20Z"}`)

	t.Run("should handle publishing an event", func(t *testing.T) {
		publisher := &fixture.MessagePublisher{}
		serializer := &fixture.Serializer{Serialized: serializedEvent}
		forwarder := event.NewForwarder(publisher, serializer)

		require.NoError(t, forwarder.Handle(ctx, testEvent))
		assert.Equal(t, []fixture.PublishedMessage{
			{Key: "SomethingHappened", Message: serializedEvent},
		}, publisher.Published())
	})

	t.Run("should fail publishing an event when the serialization fails", func(t *testing.T) {
		publisher := &fixture.MessagePublisher{}
		serializer := &fixture.Serializer{SerializeErr: errors.New("failed to serialize the event")}
		forwarder := event.NewForwarder(publisher, serializer)

		require.Error(t, forwarder.Handle(ctx, testEvent))
		assert.Empty(t, publisher.Published())
	})

	t.Run("should fail publishing an event when the message publisher fails", func(t *testing.T) {
		publisher := &fixture.MessagePublisher{Err: errors.New("failed to publish the message")}
		serializer := &fixture.Serializer{Serialized: serializedEvent}
		forwarder := event.NewForwarder(publisher, serializer)

		require.Error(t, forwarder.Handle(ctx, testEvent))
		assert.Empty(t, publisher.Published())
	})
}
