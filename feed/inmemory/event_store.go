// Package inmemory provides in-memory implementations of the event feed
// stores. They keep all data in memory and are intended for testing and local
// development; data does not survive a restart.
package inmemory

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/feed"
)

// EventStore is an in-memory implementation of feed.EventStore.
type EventStore struct {
	serializer event.Serializer

	mu           sync.RWMutex
	storedEvents []*feed.StoredEvent
	// lastID is the id given to the last appended event, deleted or not, so
	// that the ids keep increasing after a deletion.
	lastID int64
}

// NewEventStore creates a new in-memory event store.
func NewEventStore(serializer event.Serializer) *EventStore {
	return &EventStore{
		serializer: serializer,
	}
}

// Append appends a new domain event in the event store. Events are assigned a
// 1-based, monotonically increasing identity in insertion order. Deleting
// events does not reset the identity: the next event is given an identity
// greater than the ones given so far.
func (es *EventStore) Append(_ context.Context, e event.Event) error {
	body, err := es.serializer.Serialize(e)
	if err != nil {
		return fmt.Errorf("failed to append the event in the event store: %w", err)
	}
	es.mu.Lock()
	defer es.mu.Unlock()
	es.lastID++
	es.storedEvents = append(es.storedEvents, feed.NewStoredEvent(es.lastID, event.Name(e), e.OccurredOn().UTC(), body))
	return nil
}

// StoredEventsAfter returns at most limit stored events whose ID is greater
// than the provided event ID, in ascending ID order.
func (es *EventStore) StoredEventsAfter(_ context.Context, eventID int64, limit int) ([]*feed.StoredEvent, error) {
	if limit <= 0 {
		return nil, nil
	}
	es.mu.RLock()
	defer es.mu.RUnlock()
	first := es.indexOf(eventID + 1)
	last := min(first+limit, len(es.storedEvents))
	return slices.Clone(es.storedEvents[first:last]), nil
}

// LastStoredEventID returns the ID of the last stored event, and 0 when the
// store is empty.
func (es *EventStore) LastStoredEventID(_ context.Context) (int64, error) {
	es.mu.RLock()
	defer es.mu.RUnlock()
	if len(es.storedEvents) == 0 {
		return 0, nil
	}
	return es.storedEvents[len(es.storedEvents)-1].ID(), nil
}

// FirstStoredEventIDSince returns the lowest ID among the stored events that
// occurred at or after the given time, and 0 when there is none.
func (es *EventStore) FirstStoredEventIDSince(_ context.Context, since time.Time) (int64, error) {
	es.mu.RLock()
	defer es.mu.RUnlock()
	for _, storedEvent := range es.storedEvents {
		if !storedEvent.OccurredOn().Before(since) {
			return storedEvent.ID(), nil
		}
	}
	return 0, nil
}

// DeleteAllStoredEvents deletes every stored event.
func (es *EventStore) DeleteAllStoredEvents(_ context.Context) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.storedEvents = nil
	return nil
}

// KeepLastStoredEvents deletes every stored event but the count last ones.
// With a count of 0 or lower, every stored event is deleted.
func (es *EventStore) KeepLastStoredEvents(_ context.Context, count int) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	kept := max(count, 0)
	if kept >= len(es.storedEvents) {
		return nil
	}
	es.deleteFirst(len(es.storedEvents) - kept)
	return nil
}

// DeleteStoredEventsBefore deletes the stored events whose ID is lower than
// the provided event ID.
func (es *EventStore) DeleteStoredEventsBefore(_ context.Context, eventID int64) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.deleteFirst(es.indexOf(eventID))
	return nil
}

// DeleteStoredEventsOccurredBefore deletes the stored events that occurred
// before the given time, wherever they are in the store.
func (es *EventStore) DeleteStoredEventsOccurredBefore(_ context.Context, before time.Time) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.storedEvents = slices.DeleteFunc(es.storedEvents, func(storedEvent *feed.StoredEvent) bool {
		return storedEvent.OccurredOn().Before(before)
	})
	return nil
}

// indexOf returns the index of the first stored event whose ID is at least
// the provided event ID, and the number of stored events when there is none.
// The caller holds the lock.
func (es *EventStore) indexOf(eventID int64) int {
	index, _ := slices.BinarySearchFunc(es.storedEvents, eventID, func(e *feed.StoredEvent, id int64) int {
		return cmp.Compare(e.ID(), id)
	})
	return index
}

// deleteFirst deletes the n first stored events. The caller holds the lock.
func (es *EventStore) deleteFirst(n int) {
	if n <= 0 {
		return
	}
	es.storedEvents = slices.Delete(es.storedEvents, 0, n)
}

// EventStore implements the feed.EventStore interface.
var _ feed.EventStore = &EventStore{}
