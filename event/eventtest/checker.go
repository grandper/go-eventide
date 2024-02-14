package eventtest

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/grandper/go-eventide/event"
)

// Checker is an event.Subscriber that records the events it receives so they
// can be asserted on in tests. It can be registered in any scope (via the
// subscribers argument of event.Scope, event.PrivateScope or Scope) or
// subscribed to the publisher of a context with Subscribe, and is safe for
// concurrent use.
//
// Events are compared with reflect.DeepEqual: two events match when they have
// the same type and deeply equal content, whether they are values or pointers,
// and whatever the type of their fields (slices and maps included).
//
// Events that are Protocol Buffers messages are compared with proto.Equal
// instead: serializing a message writes to its internal state, so a message
// that was forwarded or stored no longer deeply equals a fresh one with the
// same content.
type Checker struct {
	mu     sync.Mutex
	events []event.Event
}

// NewChecker creates a new event checker.
func NewChecker() *Checker {
	return &Checker{}
}

// Subscribe creates a new event checker and subscribes it to the publisher of
// the context, for code under test that already runs inside a scope. It fails
// when the context carries no publisher.
func Subscribe(ctx context.Context) (*Checker, error) {
	checker := NewChecker()
	if err := event.Subscribe(ctx, checker); err != nil {
		return nil, fmt.Errorf("failed to subscribe a new checker: %w", err)
	}
	return checker, nil
}

// Handle records the event. It always succeeds.
func (c *Checker) Handle(_ context.Context, e event.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
	return nil
}

// Events returns a copy of the events recorded so far, in publication order.
func (c *Checker) Events() []event.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	events := make([]event.Event, len(c.events))
	copy(events, c.events)
	return events
}

// NumEvents returns the number of recorded events.
func (c *Checker) NumEvents() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.events)
}

// Contains returns true if the given event has been recorded.
func (c *Checker) Contains(target event.Event) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.events {
		if equal(e, target) {
			return true
		}
	}
	return false
}

// AssertEvents asserts that exactly the expected events were recorded, in order.
func (c *Checker) AssertEvents(t testing.TB, expectedEvents []event.Event) bool {
	t.Helper()
	events := c.Events()
	numEvents := len(events)
	numExpectedEvents := len(expectedEvents)
	if numEvents > numExpectedEvents {
		return assert.Fail(
			t,
			"more events than expected occurred",
			"expected %d but got %d",
			numExpectedEvents,
			numEvents,
		)
	}
	if numEvents < numExpectedEvents {
		return assert.Fail(
			t,
			"less events than expected occurred",
			"expected %d but got %d",
			numExpectedEvents,
			numEvents,
		)
	}
	for i, expectedEvent := range expectedEvents {
		if equal(expectedEvent, events[i]) {
			continue
		}
		return assert.Failf(
			t,
			"another event was expected",
			"at index %d, expected %s %+v but got %s %+v",
			i,
			event.Name(expectedEvent),
			expectedEvent,
			event.Name(events[i]),
			events[i],
		)
	}
	return true
}

// AssertContains asserts that the given event was recorded.
func (c *Checker) AssertContains(t testing.TB, target event.Event) bool {
	t.Helper()
	if !c.Contains(target) {
		return assert.Failf(
			t,
			"expected event was not recorded",
			"expected to find %s %+v",
			event.Name(target),
			target,
		)
	}
	return true
}

// equal reports whether two events match: with proto.Equal when both are
// Protocol Buffers messages, and with reflect.DeepEqual otherwise.
func equal(a, b event.Event) bool {
	messageA, isMessageA := a.(proto.Message)
	messageB, isMessageB := b.(proto.Message)
	if isMessageA && isMessageB {
		return proto.Equal(messageA, messageB)
	}
	return reflect.DeepEqual(a, b)
}

// Checker implements the event.Subscriber interface.
var _ event.Subscriber = &Checker{}
