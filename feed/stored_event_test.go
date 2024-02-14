package feed_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/feed"
	"github.com/grandper/go-eventide/internal/fixture"
)

func TestStoredEvent(t *testing.T) {
	e := fixture.NewSomethingHappened()
	eventType := event.Name(e)
	occurredOn := time.Now()
	eventBody := `{"occurredOn":"2024-06-12T01:10:20Z"}`
	const eventID int64 = 10

	t.Run("should be created from parameters", func(t *testing.T) {
		se := feed.NewStoredEvent(eventID, eventType, occurredOn, []byte(eventBody))
		assert.Equal(t, eventType, se.Type())
		assert.Equal(t, occurredOn, se.OccurredOn())
		assert.Equal(t, []byte(eventBody), se.Body())
		assert.Equal(t, eventID, se.ID())
	})
}
