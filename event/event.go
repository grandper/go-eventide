package event

import (
	"reflect"
	"time"
)

// Event is the interface implemented by all events.
type Event interface {
	// OccurredOn returns when the event occurred.
	OccurredOn() time.Time
}

// Name returns the name of the event. It is the short type name of the
// event (without its package path) and is used as the routing key when
// forwarding events and as the type name when storing them.
func Name(e Event) string {
	t := reflect.TypeOf(e)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Name()
}
