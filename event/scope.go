package event

import (
	"context"
)

// ScopedFunc is the function a scope runs. Its context holds the publisher of
// the scope.
type ScopedFunc func(ctx context.Context)

// PrivateScopeWithContext runs fn in a self-contained scope, keeping the values, deadline and cancellation of the provided context. The
// scope gets a fresh publisher: events published inside it do not reach the
// publisher of an enclosing scope. Use Scope for that.
func PrivateScopeWithContext(ctx context.Context, fn ScopedFunc, subscribers ...Subscriber) {
	p := NewPublisher()
	for _, subscriber := range subscribers {
		p.Subscribe(subscriber)
	}
	publishingCtx := ContextWithPublisher(ctx, p)
	fn(publishingCtx)
}

// PrivateScope runs fn in a self-contained scope, from a background context:
// the events published inside it are handed to the subscribers of the scope,
// and to no one else.
func PrivateScope(fn ScopedFunc, subscribers ...Subscriber) {
	PrivateScopeWithContext(context.Background(), fn, subscribers...)
}

// Scope runs fn in a scope nested in the scope of the context: the events
// published inside it are handed to the subscribers of the scope, then to the
// enclosing scope. It behaves like PrivateScopeWithContext when the context
// holds no publisher.
func Scope(ctx context.Context, fn ScopedFunc, subscribers ...Subscriber) {
	p := PublisherFromContext(ctx)
	if p == nil {
		PrivateScopeWithContext(ctx, fn, subscribers...)
		return
	}
	ft := followThrough{
		publisher: p,
	}
	// Copy into a fresh slice so we never append into the caller's backing array.
	scoped := make([]Subscriber, 0, len(subscribers)+1)
	scoped = append(scoped, subscribers...)
	scoped = append(scoped, ft)
	PrivateScopeWithContext(ctx, fn, scoped...)
}

type followThrough struct {
	publisher *Publisher
}

// Handle propagates the event to the publisher of the parent scope.
func (ft followThrough) Handle(ctx context.Context, e Event) error {
	return ft.publisher.Publish(ctx, e)
}
