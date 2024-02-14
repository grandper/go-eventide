package fixture

import (
	"context"
	"sync"

	"github.com/grandper/go-eventide/event"
)

// Subscriber is a test double that records the events it handles, along with
// the context they were handled with. Set Err to make Handle fail without
// recording the event.
type Subscriber struct {
	mutex    sync.Mutex
	handled  []event.Event
	contexts []context.Context

	// Err is returned by Handle when non-nil.
	Err error
}

// Handle records the event and its context, or returns Err when it is set.
func (s *Subscriber) Handle(ctx context.Context, e event.Event) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.Err != nil {
		return s.Err
	}
	s.handled = append(s.handled, e)
	s.contexts = append(s.contexts, ctx)
	return nil
}

// Contexts returns the contexts the events have been handled with, in order.
func (s *Subscriber) Contexts() []context.Context {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return append([]context.Context(nil), s.contexts...)
}

// Handled returns the events that have been handled, in order.
func (s *Subscriber) Handled() []event.Event {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return append([]event.Event(nil), s.handled...)
}

// Count returns the number of events that have been handled.
func (s *Subscriber) Count() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.handled)
}

// Subscriber implements the event.Subscriber interface.
var _ event.Subscriber = &Subscriber{}
