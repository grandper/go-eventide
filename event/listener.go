package event

import (
	"context"
	"errors"
	"fmt"
)

// MessageHandlerFunc is a function that handles a message. The context is the
// one the message listener hands over for that message: it carries the values,
// deadline and cancellation of the delivery (a trace, a processing deadline),
// and must be derived from the context passed to ListenToMessages.
type MessageHandlerFunc func(ctx context.Context, key string, message []byte) error

// Listener receives the events other services forwarded to a message broker,
// and hands them to subscribers.
type Listener struct {
	messageListener MessageListener
	serializer      Serializer
}

// NewListener returns a new event listener. The serializer turns the messages
// back into events: the types of the events the listener is interested in are
// registered on it.
func NewListener(messageListener MessageListener, serializer Serializer) *Listener {
	return &Listener{
		messageListener: messageListener,
		serializer:      serializer,
	}
}

// ListenToEvents listens to the messages of the given topics, deserializes
// them into events and hands each event to the subscribers, in the order they
// are given, with the context the message listener provides for the message.
//
// A message whose event type is not registered on the serializer is skipped.
// The handling of a message fails when it cannot be deserialized or when a
// subscriber fails, in which case the following subscribers do not receive
// the event: the message listener decides what happens to the message.
func (l *Listener) ListenToEvents(ctx context.Context, topicNames []string, subscribers ...Subscriber) error {
	handleMessage := func(ctx context.Context, key string, message []byte) error {
		e, err := Deserialize(l.serializer, message)
		if errors.Is(err, ErrTypeNotRegistered) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to handle the message with key '%s': %w", key, err)
		}
		for _, subscriber := range subscribers {
			if handleErr := subscriber.Handle(ctx, e); handleErr != nil {
				return fmt.Errorf("failed to handle the message with key '%s': %w", key, handleErr)
			}
		}
		return nil
	}
	return l.messageListener.ListenToMessages(ctx, topicNames, handleMessage)
}
