package feed_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/grandper/go-eventide/feed"
)

func TestBatch(t *testing.T) {
	t.Run("should be created from parameters", func(t *testing.T) {
		events := []*feed.StoredEvent{
			feed.NewStoredEvent(10, "SomethingHappened", time.Now(), []byte(`{}`)),
		}

		batch := feed.NewBatch(events, 10, true)
		assert.Equal(t, events, batch.Events())
		assert.Equal(t, int64(10), batch.Next())
		assert.True(t, batch.HasMore())
	})

	t.Run("should never hold nil events", func(t *testing.T) {
		batch := feed.NewBatch(nil, 45, false)
		assert.NotNil(t, batch.Events())
		assert.Empty(t, batch.Events())
		assert.Equal(t, int64(45), batch.Next())
		assert.False(t, batch.HasMore())
	})
}
