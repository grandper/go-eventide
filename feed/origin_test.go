package feed_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/grandper/go-eventide/feed"
)

func TestOrigin(t *testing.T) {
	t.Run("should begin at the start of the feed", func(t *testing.T) {
		origin := feed.FromStart()
		assert.Equal(t, feed.OriginStart, origin.Kind())
		assert.Equal(t, int64(0), origin.Position())
		assert.True(t, origin.Time().IsZero())
	})

	t.Run("should begin at the start of the feed by default", func(t *testing.T) {
		var origin feed.Origin
		assert.Equal(t, feed.FromStart(), origin)
	})

	t.Run("should begin at the end of the feed", func(t *testing.T) {
		assert.Equal(t, feed.OriginEnd, feed.FromEnd().Kind())
	})

	t.Run("should begin at a time", func(t *testing.T) {
		since := time.Date(2026, time.September, 26, 10, 0, 0, 0, time.UTC)
		origin := feed.Since(since)
		assert.Equal(t, feed.OriginTime, origin.Kind())
		assert.Equal(t, since, origin.Time())
	})

	t.Run("should begin after a position", func(t *testing.T) {
		origin := feed.After(45)
		assert.Equal(t, feed.OriginPosition, origin.Kind())
		assert.Equal(t, int64(45), origin.Position())
	})

	t.Run("should begin at the start of the feed after the position 0", func(t *testing.T) {
		assert.Equal(t, feed.FromStart(), feed.After(0))
	})
}
