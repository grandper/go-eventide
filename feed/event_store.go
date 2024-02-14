package feed

import (
	"context"
	"time"

	"github.com/grandper/go-eventide/event"
)

// EventStore keeps the events, in the order they were stored.
//
// The ids of the stored events are positive and increasing, in the order the
// events were stored. Gaps are allowed. An event never becomes visible with an
// id lower than the id of an event that is already visible: a reader that went
// past an id never looks behind it.
//
// Deleting events never resets the ids: an event appended afterwards is given
// an id greater than the ids of the events deleted before it. A reader that has
// not reached the deleted events yet never receives them.
type EventStore interface {
	// Append serializes the event and stores it under the next id.
	Append(ctx context.Context, e event.Event) error

	// StoredEventsAfter returns at most limit stored events whose id is
	// greater than eventID, in ascending id order.
	StoredEventsAfter(ctx context.Context, eventID int64, limit int) ([]*StoredEvent, error)

	// LastStoredEventID returns the id of the last stored event, and 0 when
	// the store is empty.
	LastStoredEventID(ctx context.Context) (int64, error)

	// FirstStoredEventIDSince returns the lowest id among the stored events
	// that occurred at or after the given time, and 0 when there is none.
	FirstStoredEventIDSince(ctx context.Context, since time.Time) (int64, error)

	// DeleteAllStoredEvents deletes every stored event.
	DeleteAllStoredEvents(ctx context.Context) error

	// KeepLastStoredEvents deletes every stored event but the count last
	// ones, the ones with the highest ids. With a count of 0 or lower, every
	// stored event is deleted.
	KeepLastStoredEvents(ctx context.Context, count int) error

	// DeleteStoredEventsBefore deletes the stored events whose id is lower
	// than eventID.
	DeleteStoredEventsBefore(ctx context.Context, eventID int64) error

	// DeleteStoredEventsOccurredBefore deletes the stored events that
	// occurred before the given time, wherever they are in the store: the
	// events are not always stored in the order they occurred.
	DeleteStoredEventsOccurredBefore(ctx context.Context, before time.Time) error
}
