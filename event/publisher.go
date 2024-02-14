package event

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// Publisher delivers the events it publishes to its subscribers. It is safe
// for concurrent use.
type Publisher struct {
	mutex       sync.RWMutex
	subscribers []Subscriber
}

// NewPublisher creates a new publisher.
func NewPublisher() *Publisher {
	return &Publisher{
		subscribers: []Subscriber{},
	}
}

// Publish delivers an event to all the subscribers, in the order they
// subscribed. It stops at the first subscriber that fails and returns its
// error. The context is
// handed to every subscriber. Delivery stops, with the context's error, as
// soon as the context is done, so the subscribers registered after that point
// do not receive the event.
//
// The event is delivered to the subscribers registered when the call starts:
// the publisher is not locked during the delivery, so a subscriber may publish,
// subscribe, or reset the publisher from inside Handle. A subscriber added
// during the delivery does not receive the event being delivered, and a Reset
// does not interrupt it.
func (p *Publisher) Publish(ctx context.Context, e Event) error {
	// The slice is never modified in place (see Subscribe), so it can be
	// read after the lock is released.
	p.mutex.RLock()
	subscribers := p.subscribers
	p.mutex.RUnlock()
	for _, subscriber := range subscribers {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("failed to publish the event: %w", err)
		}
		if err := subscriber.Handle(ctx, e); err != nil {
			return fmt.Errorf("failed to publish the event: %w", err)
		}
	}
	return nil
}

// Reset removes all the subscribers.
func (p *Publisher) Reset() {
	p.mutex.Lock()
	p.subscribers = []Subscriber{}
	p.mutex.Unlock()
}

// SubscriberCount returns the number of subscribers.
func (p *Publisher) SubscriberCount() int {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return len(p.subscribers)
}

// Subscribe subscribes a subscriber to the publisher.
func (p *Publisher) Subscribe(subscriber Subscriber) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	// The subscribers are copied to a new slice rather than appended in
	// place, so the deliveries in progress keep reading their own slice.
	p.subscribers = append(slices.Clone(p.subscribers), subscriber)
}

type contextKey struct{}

//nolint:gochecknoglobals // a context key must be a stable package-level singleton.
var activePublisherKey = contextKey{}

// ContextWithPublisher returns a new `context.Context` that holds a reference to
// the event publisher. If publisher is nil, ctx is returned unchanged.
func ContextWithPublisher(ctx context.Context, publisher *Publisher) context.Context {
	if publisher != nil {
		return context.WithValue(ctx, activePublisherKey, publisher)
	}
	return ctx
}

// PublisherFromContext returns the `Publisher` previously associated with `ctx`, or
// `nil` if no such `Publisher` could be found.
func PublisherFromContext(ctx context.Context) *Publisher {
	val := ctx.Value(activePublisherKey)
	if publisher, ok := val.(*Publisher); ok {
		return publisher
	}
	return nil
}

// Publish publishes an event through the publisher of the context. The context
// is handed to the subscribers along with the event. It does nothing when the
// context holds no publisher, so that a caller can always publish without
// caring whether anybody listens.
func Publish(ctx context.Context, e Event) error {
	if publisher := PublisherFromContext(ctx); publisher != nil {
		return publisher.Publish(ctx, e)
	}
	return nil
}

// Subscribe subscribes to the events of the publisher of the context. It fails
// with ErrNoPublisher when the context holds no publisher.
func Subscribe(ctx context.Context, subscriber Subscriber) error {
	if publisher := PublisherFromContext(ctx); publisher != nil {
		publisher.Subscribe(subscriber)
		return nil
	}
	return fmt.Errorf("failed to subscribe to events: %w", ErrNoPublisher)
}

// ChanSubscribe forwards the events of the publisher of the context to a
// channel the caller owns (see ChanForwarder). It fails with ErrNoPublisher
// when the context holds no publisher.
func ChanSubscribe(ctx context.Context, ch chan<- Event) error {
	return Subscribe(ctx, NewChanForwarder(ch))
}
