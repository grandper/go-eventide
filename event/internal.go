package event

import "context"

// Internal marks the events that must not leave the service. Embed it in an
// event to make it internal.
type Internal struct{}

func (i Internal) isInternal() {}

type internalEvent interface {
	isInternal()
}

// IsInternal returns true if the event is internal, that is, if it embeds
// Internal.
func IsInternal(e any) bool {
	_, isInternal := e.(internalEvent)
	return isInternal
}

type excludeInternalSubscriber struct {
	next Subscriber
}

// ExcludeInternal decorates a subscriber so that it does not receive the
// internal events.
func ExcludeInternal(subscriber Subscriber) Subscriber {
	return &excludeInternalSubscriber{
		next: subscriber,
	}
}

// Handle hands the event to the decorated subscriber, unless it is internal.
func (s *excludeInternalSubscriber) Handle(ctx context.Context, e Event) error {
	if !IsInternal(e) {
		return s.next.Handle(ctx, e)
	}
	return nil
}

// excludeInternalSubscriber implements the Subscriber interface.
var _ Subscriber = &excludeInternalSubscriber{}
