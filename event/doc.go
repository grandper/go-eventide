// Package event provides in-process domain events: events are published
// through a Publisher carried by the context and delivered synchronously, in
// order, to its subscribers. Scopes own a publisher for the duration of a
// function call, and Forwarders and Listeners carry events across the process
// boundary through a message broker.
//
// Publishing takes one line wherever a context is available:
//
//	event.PrivateScope(func(ctx context.Context) {
//		_ = event.Publish(ctx, OrderPlaced{OrderID: "42"})
//	}, event.NewLoggingSubscriber())
//
// The "Overview" and "Main Concepts" sections of the README describe how these
// pieces fit together: https://github.com/grandper/go-eventide#overview
package event
