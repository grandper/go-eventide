package event_test

import (
	"time"

	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/internal/fixture"
)

// valueEvent is an event handled by value.
type valueEvent struct{}

func (valueEvent) OccurredOn() time.Time { return time.Time{} }

func TestEvent(t *testing.T) {
	t.Run("should retrieve the name of events", func(t *testing.T) {
		assert.Equal(t, "valueEvent", event.Name(valueEvent{}))
	})

	t.Run("should retrieve the name of pointers to events", func(t *testing.T) {
		assert.Equal(t, "SomethingHappened", event.Name(&fixture.SomethingHappened{}))
	})
}
