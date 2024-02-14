package inmemory

import (
	"context"
	"sync"

	"github.com/grandper/go-eventide/feed"
)

// PositionStore is an in-memory implementation of feed.PositionStore.
type PositionStore struct {
	mu       sync.RWMutex
	position int64
	found    bool
}

// NewPositionStore creates a new in-memory position store. It holds no
// position at first.
func NewPositionStore() *PositionStore {
	return &PositionStore{}
}

// Position returns the saved position. found is false when no position was
// saved yet.
func (s *PositionStore) Position(_ context.Context) (int64, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.position, s.found, nil
}

// SavePosition records the position.
func (s *PositionStore) SavePosition(_ context.Context, position int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.position = position
	s.found = true
	return nil
}

// PositionStore implements the feed.PositionStore interface.
var _ feed.PositionStore = &PositionStore{}
