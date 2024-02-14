package event_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/internal/fixture"
)

func TestListener(t *testing.T) {
	ctx := context.Background()
	testEvent := fixture.NewSomethingHappened()
	topicNames := []string{"users", "shop"}
	body := []byte(`{"occurredOn":"2024-06-12T01:10:20Z"}`)

	t.Run("should hand the events of other services to its subscribers", func(t *testing.T) {
		messageListener := &fixture.MessageListener{
			Messages: []fixture.IncomingMessage{{Key: "SomethingHappened", Body: body}},
		}
		serializer := &fixture.Serializer{Deserialized: testEvent}
		listener := event.NewListener(messageListener, serializer)
		first, second := &fixture.Subscriber{}, &fixture.Subscriber{}

		require.NoError(t, listener.ListenToEvents(ctx, topicNames, first, second))
		assert.Equal(t, topicNames, messageListener.Topics)
		assert.Equal(t, []error{nil}, messageListener.HandlerErrs)
		assert.Equal(t, body, serializer.DeserializeInput)
		assert.Equal(t, []event.Event{testEvent}, first.Handled())
		assert.Equal(t, []event.Event{testEvent}, second.Handled())
	})

	t.Run("should hand the events of one type to a handling function", func(t *testing.T) {
		messageListener := &fixture.MessageListener{
			Messages: []fixture.IncomingMessage{{Key: "SomethingHappened", Body: body}},
		}
		listener := event.NewListener(messageListener, &fixture.Serializer{Deserialized: testEvent})

		var handled []*fixture.SomethingHappened
		require.NoError(t, listener.ListenToEvents(ctx, topicNames,
			event.HandledBy(func(_ context.Context, e *fixture.SomethingHappened) error {
				handled = append(handled, e)
				return nil
			}),
			event.HandledBy(func(context.Context, *fixture.SomethingElseHappened) error {
				assert.Fail(t, "this should not be called")
				return nil
			}),
		))
		assert.Equal(t, []error{nil}, messageListener.HandlerErrs)
		assert.Equal(t, []*fixture.SomethingHappened{testEvent}, handled)
	})

	t.Run("should hand the message context to the subscribers", func(t *testing.T) {
		messageListener := &fixture.MessageListener{
			Messages: []fixture.IncomingMessage{{Key: "SomethingHappened", Body: body}},
			MessageContext: func(ctx context.Context, m fixture.IncomingMessage) context.Context {
				return context.WithValue(ctx, messageKeyContextKey{}, m.Key)
			},
		}
		serializer := &fixture.Serializer{Deserialized: testEvent}
		listener := event.NewListener(messageListener, serializer)
		subscriber := &fixture.Subscriber{}

		listeningCtx := context.WithValue(ctx, testContextKey{}, "value")
		require.NoError(t, listener.ListenToEvents(listeningCtx, topicNames, subscriber))
		require.Len(t, subscriber.Contexts(), 1)
		handlingCtx := subscriber.Contexts()[0]
		assert.Equal(t, "value", handlingCtx.Value(testContextKey{}), "the listening context's values are kept")
		assert.Equal(t, "SomethingHappened", handlingCtx.Value(messageKeyContextKey{}),
			"the message context's values are handed over")
	})

	t.Run("should skip the events whose type is not registered", func(t *testing.T) {
		messageListener := &fixture.MessageListener{
			Messages: []fixture.IncomingMessage{{Key: "foobar", Body: body}},
		}
		serializer := &fixture.Serializer{
			DeserializeErr: fmt.Errorf("failed to deserialize: %w", event.ErrTypeNotRegistered),
		}
		listener := event.NewListener(messageListener, serializer)
		subscriber := &fixture.Subscriber{}

		require.NoError(t, listener.ListenToEvents(ctx, topicNames, subscriber))
		assert.Equal(t, []error{nil}, messageListener.HandlerErrs)
		assert.Empty(t, subscriber.Handled())
	})

	t.Run("should fail to handle a message that cannot be deserialized", func(t *testing.T) {
		messageListener := &fixture.MessageListener{
			Messages: []fixture.IncomingMessage{{Key: "foobar", Body: body}},
		}
		deserializeErr := errors.New("failed to deserialize event")
		listener := event.NewListener(messageListener, &fixture.Serializer{DeserializeErr: deserializeErr})
		subscriber := &fixture.Subscriber{}

		require.NoError(t, listener.ListenToEvents(ctx, topicNames, subscriber))
		require.Len(t, messageListener.HandlerErrs, 1)
		require.ErrorIs(t, messageListener.HandlerErrs[0], deserializeErr)
		assert.Contains(t, messageListener.HandlerErrs[0].Error(), "'foobar'")
		assert.Empty(t, subscriber.Handled())
	})

	t.Run("should fail to handle a message that is not an event", func(t *testing.T) {
		messageListener := &fixture.MessageListener{
			Messages: []fixture.IncomingMessage{{Key: "foobar", Body: body}},
		}
		listener := event.NewListener(messageListener, &fixture.Serializer{Deserialized: "not an event"})
		subscriber := &fixture.Subscriber{}

		require.NoError(t, listener.ListenToEvents(ctx, topicNames, subscriber))
		require.Len(t, messageListener.HandlerErrs, 1)
		require.ErrorIs(t, messageListener.HandlerErrs[0], event.ErrNotAnEvent)
		assert.Empty(t, subscriber.Handled())
	})

	t.Run("should fail to handle a message when a subscriber fails", func(t *testing.T) {
		messageListener := &fixture.MessageListener{
			Messages: []fixture.IncomingMessage{{Key: "SomethingHappened", Body: body}},
		}
		listener := event.NewListener(messageListener, &fixture.Serializer{Deserialized: testEvent})
		handlingErr := errors.New("failed to handle the event")
		failing, next := &fixture.Subscriber{Err: handlingErr}, &fixture.Subscriber{}

		require.NoError(t, listener.ListenToEvents(ctx, topicNames, failing, next))
		require.Len(t, messageListener.HandlerErrs, 1)
		require.ErrorIs(t, messageListener.HandlerErrs[0], handlingErr)
		assert.Empty(t, next.Handled(), "the next subscribers do not receive the event")
	})
}

// messageKeyContextKey is the context key under which the test message listener
// stores the key of the message being delivered.
type messageKeyContextKey struct{}
