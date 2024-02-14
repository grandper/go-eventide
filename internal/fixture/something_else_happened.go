//nolint:dupl // SomethingHappened and SomethingElseHappened are two sample events with the same shape on purpose.
package fixture

import (
	"encoding/json"
	"time"

	"github.com/grandper/go-eventide/event"
)

// SomethingElseHappened is a domain event published when something else
// happened. It is always handled as a pointer: its methods use pointer
// receivers so that the event stays a value object with unexported fields.
type SomethingElseHappened struct {
	occurredOn time.Time
}

// somethingElseHappenedJSON is the JSON representation of SomethingElseHappened.
type somethingElseHappenedJSON struct {
	OccurredOn time.Time `json:"occurredOn"`
}

// NewSomethingElseHappened creates a new SomethingElseHappened event.
func NewSomethingElseHappened() *SomethingElseHappened {
	return &SomethingElseHappened{
		occurredOn: time.Date(2024, time.June, 13, 2, 20, 30, 0, time.UTC),
	}
}

// OccurredOn returns when the event occurred.
func (e *SomethingElseHappened) OccurredOn() time.Time {
	return e.occurredOn
}

// MarshalJSON serializes the event to JSON.
func (e *SomethingElseHappened) MarshalJSON() ([]byte, error) {
	return json.Marshal(somethingElseHappenedJSON{OccurredOn: e.occurredOn})
}

// UnmarshalJSON deserializes the event from JSON.
func (e *SomethingElseHappened) UnmarshalJSON(data []byte) error {
	var v somethingElseHappenedJSON
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	e.occurredOn = v.OccurredOn
	return nil
}

// SomethingElseHappened implements the event.Event interface.
var _ event.Event = &SomethingElseHappened{}
