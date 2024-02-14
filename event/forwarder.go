package event

import (
	"context"
	"fmt"
)

// Forwarder is a subscriber that forwards the events to other services: it
// serializes them and publishes them through a message publisher, under the
// name of the event as routing key.
type Forwarder struct {
	publisher  MessagePublisher
	serializer Serializer
}

// NewForwarder returns a new Forwarder.
func NewForwarder(publisher MessagePublisher, serializer Serializer) *Forwarder {
	return &Forwarder{
		publisher:  publisher,
		serializer: serializer,
	}
}

// Handle handles the event by forwarding it using a message publisher. The
// context is passed on to the message publisher, so the publication is bound
// by the deadline and cancellation of the caller. Wrap the forwarder to
// publish with context.WithoutCancel(ctx) when the events must be forwarded
// even after the caller gives up.
func (f *Forwarder) Handle(ctx context.Context, e Event) error {
	serializedEvent, err := f.serializer.Serialize(e)
	if err != nil {
		return fmt.Errorf("failed to forward the event '%s': %w", Name(e), err)
	}
	return f.publisher.Publish(ctx, Name(e), serializedEvent)
}
