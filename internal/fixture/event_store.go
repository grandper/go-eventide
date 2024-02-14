package fixture

import (
	"context"
	"time"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/feed"
)

// AfterCall is a call to StoredEventsAfter recorded by EventStore.
type AfterCall struct {
	// EventID is the event ID the call read after.
	EventID int64
	// Limit is the maximum number of events the call asked for.
	Limit int
}

// EventStore is a test double for feed.EventStore. It records appended events,
// the readings and the deletions, and returns the preset values configured on
// its fields.
type EventStore struct {
	// Appended captures the events passed to Append, in order.
	Appended []event.Event
	// AppendErr is returned by Append when it is set.
	AppendErr error

	// After is returned by StoredEventsAfter, cut at the limit of the call.
	After []*feed.StoredEvent
	// AfterErr is returned by StoredEventsAfter when it is set.
	AfterErr error
	// AfterCalls captures the calls to StoredEventsAfter.
	AfterCalls []AfterCall

	// LastID is returned by LastStoredEventID.
	LastID int64
	// LastIDErr is returned by LastStoredEventID.
	LastIDErr error

	// FirstIDSince is returned by FirstStoredEventIDSince.
	FirstIDSince int64
	// FirstIDSinceErr is returned by FirstStoredEventIDSince.
	FirstIDSinceErr error
	// FirstIDSinceCalls captures the times passed to FirstStoredEventIDSince.
	FirstIDSinceCalls []time.Time

	// DeleteErr is returned by every deletion method when it is set.
	DeleteErr error
	// DeleteAllCalls counts the calls to DeleteAllStoredEvents.
	DeleteAllCalls int
	// KeepLastCalls captures the counts passed to KeepLastStoredEvents.
	KeepLastCalls []int
	// DeleteBeforeCalls captures the event IDs passed to DeleteStoredEventsBefore.
	DeleteBeforeCalls []int64
	// DeleteOccurredBeforeCalls captures the times passed to
	// DeleteStoredEventsOccurredBefore.
	DeleteOccurredBeforeCalls []time.Time
}

// Append records the event, or returns AppendErr when it is set.
func (s *EventStore) Append(_ context.Context, e event.Event) error {
	if s.AppendErr != nil {
		return s.AppendErr
	}
	s.Appended = append(s.Appended, e)
	return nil
}

// StoredEventsAfter records the call and returns the preset After/AfterErr.
func (s *EventStore) StoredEventsAfter(_ context.Context, eventID int64, limit int) ([]*feed.StoredEvent, error) {
	s.AfterCalls = append(s.AfterCalls, AfterCall{EventID: eventID, Limit: limit})
	if s.AfterErr != nil {
		return nil, s.AfterErr
	}
	return s.After[:min(limit, len(s.After))], nil
}

// LastStoredEventID returns the preset LastID/LastIDErr.
func (s *EventStore) LastStoredEventID(_ context.Context) (int64, error) {
	return s.LastID, s.LastIDErr
}

// FirstStoredEventIDSince records the call and returns the preset
// FirstIDSince/FirstIDSinceErr.
func (s *EventStore) FirstStoredEventIDSince(_ context.Context, since time.Time) (int64, error) {
	s.FirstIDSinceCalls = append(s.FirstIDSinceCalls, since)
	return s.FirstIDSince, s.FirstIDSinceErr
}

// DeleteAllStoredEvents counts the call and returns the preset DeleteErr.
func (s *EventStore) DeleteAllStoredEvents(_ context.Context) error {
	s.DeleteAllCalls++
	return s.DeleteErr
}

// KeepLastStoredEvents records the call and returns the preset DeleteErr.
func (s *EventStore) KeepLastStoredEvents(_ context.Context, count int) error {
	s.KeepLastCalls = append(s.KeepLastCalls, count)
	return s.DeleteErr
}

// DeleteStoredEventsBefore records the call and returns the preset DeleteErr.
func (s *EventStore) DeleteStoredEventsBefore(_ context.Context, eventID int64) error {
	s.DeleteBeforeCalls = append(s.DeleteBeforeCalls, eventID)
	return s.DeleteErr
}

// DeleteStoredEventsOccurredBefore records the call and returns the preset
// DeleteErr.
func (s *EventStore) DeleteStoredEventsOccurredBefore(_ context.Context, before time.Time) error {
	s.DeleteOccurredBeforeCalls = append(s.DeleteOccurredBeforeCalls, before)
	return s.DeleteErr
}

// EventStore implements the feed.EventStore interface.
var _ feed.EventStore = &EventStore{}
