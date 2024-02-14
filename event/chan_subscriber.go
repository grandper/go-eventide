package event

import (
	"context"
	"fmt"
	"sync"
)

// ChanSubscriber is a subscriber that delivers events on a channel it owns.
// Unlike ChanForwarder, which forwards events to a caller-owned channel,
// the ChanSubscriber creates and owns its channel: closing the subscriber
// closes the channel, terminating range loops on the consumer side.
//
// Handle blocks while the channel buffer is full, which in turn blocks the
// publisher, until the subscriber is closed or the context is done. Consume
// events from a separate goroutine, or use a capacity large enough for the
// expected number of events.
type ChanSubscriber struct {
	mutex sync.RWMutex
	once  sync.Once
	ch    chan Event
	done  chan struct{}
}

// NewChanSubscriber creates a ChanSubscriber whose channel has the given
// capacity and subscribes it to the publisher held by the context. It fails
// with ErrNoPublisher when the context holds no publisher.
func NewChanSubscriber(ctx context.Context, capacity int) (*ChanSubscriber, error) {
	s := &ChanSubscriber{
		ch:   make(chan Event, capacity),
		done: make(chan struct{}),
	}
	if err := Subscribe(ctx, s); err != nil {
		return nil, fmt.Errorf("failed to create a new chan subscriber: %w", err)
	}
	return s, nil
}

// Ch returns the channel on which events are delivered. The channel is
// closed when the subscriber is closed, so it is safe to range over it.
func (s *ChanSubscriber) Ch() <-chan Event {
	return s.ch
}

// Handle delivers the event on the subscriber's channel. It blocks while
// the channel buffer is full, and gives up with the context's error when the
// context is done first. Events handled after the subscriber has been closed
// are silently dropped.
func (s *ChanSubscriber) Handle(ctx context.Context, e Event) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	select {
	case <-s.done:
		return nil
	default:
	}
	select {
	case s.ch <- e:
		return nil
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close closes the subscriber and its channel, unblocking any pending
// Handle call. It is safe to call Close multiple times and while events
// are being published.
func (s *ChanSubscriber) Close() {
	s.once.Do(func() {
		// Closing done first unblocks the Handle calls parked in their
		// send, so they release their read locks and the write lock can
		// be acquired. The channel is only closed under the write lock,
		// which no Handle call can be holding a read lock through.
		close(s.done)
		s.mutex.Lock()
		close(s.ch)
		s.mutex.Unlock()
	})
}

// ChanSubscriber implements the Subscriber interface.
var _ Subscriber = &ChanSubscriber{}
