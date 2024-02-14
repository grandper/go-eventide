package inmemory_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/feed/inmemory"
)

func TestPositionStore(t *testing.T) {
	ctx := context.Background()

	t.Run("should hold no position at first", func(t *testing.T) {
		store := inmemory.NewPositionStore()

		position, found, err := store.Position(ctx)
		require.NoError(t, err)
		assert.False(t, found)
		assert.Equal(t, int64(0), position)
	})

	t.Run("should save and return the position", func(t *testing.T) {
		store := inmemory.NewPositionStore()
		require.NoError(t, store.SavePosition(ctx, 45))

		position, found, err := store.Position(ctx)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, int64(45), position)
	})

	t.Run("should tell the position 0 from no position", func(t *testing.T) {
		store := inmemory.NewPositionStore()
		require.NoError(t, store.SavePosition(ctx, 0))

		position, found, err := store.Position(ctx)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, int64(0), position)
	})
}
