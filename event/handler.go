package event

import (
	"context"
)

// HandlingFunc is a function that handles the events of type T, which must be
// an Event: a function for a type that is not one is rejected at compile
// time, rather than never called. The context is the one the event was
// published with.
type HandlingFunc[T Event] func(ctx context.Context, event T) error

// Handle subscribes a handling function to the events of type T of the
// publisher of the context. It fails with ErrNoPublisher when the context
// holds no publisher.
func Handle[T Event](ctx context.Context, fn HandlingFunc[T]) error {
	return Subscribe(ctx, HandledBy(fn))
}

// HandledBy returns a subscriber of the events of type T: it hands them to the
// handling function, and ignores the events of any other type.
func HandledBy[T Event](fn HandlingFunc[T]) Subscriber {
	return handler[T]{
		fn: fn,
	}
}

type handler[T Event] struct {
	fn HandlingFunc[T]
}

// Handle handles the event of type T with the handling function.
func (h handler[T]) Handle(ctx context.Context, e Event) error {
	switch v := e.(type) {
	case T:
		return h.fn(ctx, v)
	default:
		return nil
	}
}
