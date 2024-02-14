package main

import (
	"context"
	"log"
	"time"

	"github.com/grandper/go-eventide/event"
)

// HelloEvent is a domain event published when we want to say hello.
type HelloEvent struct {
	occurredOn time.Time
}

// NewHelloEvent creates a new HelloEvent.
func NewHelloEvent() *HelloEvent {
	return &HelloEvent{occurredOn: time.Now()}
}

// OccurredOn reports when the event happened, satisfying event.Event.
func (e *HelloEvent) OccurredOn() time.Time {
	return e.occurredOn
}

// HelloSubscriber logs every event it receives, tagged with its layer.
type HelloSubscriber struct {
	// Layer tags the log lines, so the output tells the subscribers apart.
	Layer string
}

// Handle logs the received event. The context is the one the event was
// published with; this subscriber has no use for it.
func (s *HelloSubscriber) Handle(_ context.Context, e event.Event) error {
	log.Printf("[%s] received %s", s.Layer, event.Name(e))
	return nil
}

// serverCall owns the outermost scope. A subscriber registered here observes
// every event published while handling the request, however deeply nested.
func serverCall(ctx context.Context) {
	event.PrivateScopeWithContext(ctx, applicationCall, &HelloSubscriber{Layer: "server"})
}

// applicationCall opens a sub-scope with its own subscriber. Events published
// inside the sub-scope are also propagated up to the server scope.
func applicationCall(ctx context.Context) {
	event.Scope(ctx, domainCall, &HelloSubscriber{Layer: "application"})
}

// domainCall is the deepest layer. It just publishes; it neither knows nor
// cares which subscribers are listening.
func domainCall(ctx context.Context) {
	if err := event.Publish(ctx, NewHelloEvent()); err != nil {
		log.Fatalf("publish HelloEvent: %v", err)
	}
}

func main() {
	serverCall(context.Background())
}
