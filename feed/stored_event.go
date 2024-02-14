package feed

import (
	"time"
)

// StoredEvent is an event as the event store keeps it: the serialized event,
// along with its place in the feed.
type StoredEvent struct {
	id         int64
	eventType  string
	occurredOn time.Time
	body       []byte
}

// NewStoredEvent creates a new stored event from its id, the name of the type
// of the event (see event.Name), the time the event occurred on, and the
// serialized event.
func NewStoredEvent(id int64, eventType string, occurredOn time.Time, body []byte) *StoredEvent {
	return &StoredEvent{
		id:         id,
		eventType:  eventType,
		occurredOn: occurredOn,
		body:       body,
	}
}

// ID returns the id of the stored event, which is its position in the feed.
func (se *StoredEvent) ID() int64 {
	return se.id
}

// Type returns the name of the type of the event.
func (se *StoredEvent) Type() string {
	return se.eventType
}

// OccurredOn returns the time when the event occurred.
func (se *StoredEvent) OccurredOn() time.Time {
	return se.occurredOn
}

// Body returns the serialized event.
func (se *StoredEvent) Body() []byte {
	return se.body
}
