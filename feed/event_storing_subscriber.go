package feed

import (
	"context"

	"github.com/grandper/go-eventide/event"
)

// EventStoringSubscriber is a subscriber that stores the events it receives
// into an EventStore.
type EventStoringSubscriber struct {
	eventStore EventStore
}

// NewEventStoringSubscriber creates a subscriber storing events into the event store.
func NewEventStoringSubscriber(eventStore EventStore) *EventStoringSubscriber {
	return &EventStoringSubscriber{
		eventStore: eventStore,
	}
}

// Handle handles the event by storing it into the EventStore. The context is
// passed on to the store, so the append is bound by the deadline and
// cancellation of the caller.
func (ess *EventStoringSubscriber) Handle(ctx context.Context, e event.Event) error {
	return ess.eventStore.Append(ctx, e)
}

// EventStoringSubscriber implements the event.Subscriber interface.
var _ event.Subscriber = &EventStoringSubscriber{}
