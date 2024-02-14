package event

import "context"

// Subscriber handles the events it receives from a publisher.
type Subscriber interface {
	// Handle handles an event. The context is the one the event was published
	// with, so it carries the values, deadline and cancellation of the caller,
	// as well as the publisher of the current scope. It is only valid for the
	// duration of the call: a subscriber that defers work to another goroutine
	// must not retain it.
	Handle(ctx context.Context, e Event) error
}
