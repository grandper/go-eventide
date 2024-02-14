//nolint:dupl // SomethingHappened and SomethingElseHappened are two sample events with the same shape on purpose.
package fixture

import (
	"encoding/json"
	"time"

	"github.com/grandper/go-eventide/event"
)

// SomethingHappened is a domain event published when something happened. It is
// always handled as a pointer: its methods use pointer receivers so that the
// event stays a value object with unexported fields.
type SomethingHappened struct {
	occurredOn time.Time
}

// somethingHappenedJSON is the JSON representation of SomethingHappened.
type somethingHappenedJSON struct {
	OccurredOn time.Time `json:"occurredOn"`
}

// NewSomethingHappened creates a new SomethingHappened event.
func NewSomethingHappened() *SomethingHappened {
	return &SomethingHappened{
		occurredOn: time.Date(2024, time.June, 12, 1, 10, 20, 0, time.UTC),
	}
}

// OccurredOn returns when the event occurred.
func (e *SomethingHappened) OccurredOn() time.Time {
	return e.occurredOn
}

// MarshalJSON serializes the event to JSON.
func (e *SomethingHappened) MarshalJSON() ([]byte, error) {
	return json.Marshal(somethingHappenedJSON{OccurredOn: e.occurredOn})
}

// UnmarshalJSON deserializes the event from JSON.
func (e *SomethingHappened) UnmarshalJSON(data []byte) error {
	var v somethingHappenedJSON
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	e.occurredOn = v.OccurredOn
	return nil
}

// SomethingHappened implements the event.Event interface.
var _ event.Event = &SomethingHappened{}
