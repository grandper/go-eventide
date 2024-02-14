package feed

import "context"

// PositionStore remembers the position in a feed of the one that follows it,
// a Consumer or a Relay: the id of the last event it handled. A position
// store holds a single position, so each follower has its own.
type PositionStore interface {
	// Position returns the saved position. found is false when no position
	// was saved yet.
	Position(ctx context.Context) (position int64, found bool, err error)

	// SavePosition records the position.
	SavePosition(ctx context.Context, position int64) error
}
