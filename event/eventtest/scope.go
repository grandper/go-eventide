package eventtest

import (
	"context"

	"github.com/grandper/go-eventide/event"
)

// Scope creates a private scope, from a background context, for asserting
// events happening. It returns a Checker recording every event published
// within the scope.
func Scope(fn event.ScopedFunc, subscribers ...event.Subscriber) *Checker {
	return ScopeWithContext(context.Background(), fn, subscribers...)
}

// ScopeWithContext creates a private scope for asserting events happening,
// keeping the values, deadline and cancellation of the provided context. It
// returns a Checker recording every event published within the scope.
func ScopeWithContext(ctx context.Context, fn event.ScopedFunc, subscribers ...event.Subscriber) *Checker {
	checker := NewChecker()
	// Copy into a fresh slice so we never append into the caller's backing array.
	scoped := make([]event.Subscriber, 0, len(subscribers)+1)
	scoped = append(scoped, subscribers...)
	scoped = append(scoped, checker)
	event.PrivateScopeWithContext(ctx, fn, scoped...)
	return checker
}
