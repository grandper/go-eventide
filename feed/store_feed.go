package feed

import (
	"context"
	"fmt"
)

// StoreFeed is the feed of an event store: it serves the events of the store,
// in the order they were stored.
type StoreFeed struct {
	eventStore EventStore
}

// NewStoreFeed creates the feed of the event store.
func NewStoreFeed(eventStore EventStore) *StoreFeed {
	return &StoreFeed{
		eventStore: eventStore,
	}
}

// RetrieveEvents returns at most limit events from the origin. A limit of 0 stands
// for DefaultLimit. It fails with ErrInvalidLimit when the limit is negative
// or above MaxLimit, and with ErrInvalidPosition when the origin is a negative
// position.
func (f *StoreFeed) RetrieveEvents(ctx context.Context, origin Origin, limit int) (*Batch, error) {
	limit, err := NormalizeLimit(limit)
	if err != nil {
		return nil, fmt.Errorf("failed to read the feed: %w", err)
	}
	position, err := f.positionOf(ctx, origin)
	if err != nil {
		return nil, fmt.Errorf("failed to read the feed: %w", err)
	}
	// One more event is read to know whether the feed goes on after the batch.
	storedEvents, err := f.eventStore.StoredEventsAfter(ctx, position, limit+1)
	if err != nil {
		return nil, fmt.Errorf("failed to read the feed: %w", err)
	}
	hasMore := len(storedEvents) > limit
	if hasMore {
		storedEvents = storedEvents[:limit]
	}
	if len(storedEvents) > 0 {
		position = storedEvents[len(storedEvents)-1].ID()
	}
	return NewBatch(storedEvents, position, hasMore), nil
}

// positionOf turns an origin into the position the reading begins after.
func (f *StoreFeed) positionOf(ctx context.Context, origin Origin) (int64, error) {
	switch origin.Kind() {
	case OriginStart:
		return 0, nil
	case OriginPosition:
		if origin.Position() < 0 {
			return 0, ErrInvalidPosition
		}
		return origin.Position(), nil
	case OriginEnd:
		return f.eventStore.LastStoredEventID(ctx)
	case OriginTime:
		return f.positionSince(ctx, origin)
	default:
		return 0, fmt.Errorf("unknown kind of origin: %d", origin.Kind())
	}
}

func (f *StoreFeed) positionSince(ctx context.Context, origin Origin) (int64, error) {
	// The end is read first: when no event matches, an event stored in
	// between comes after the position returned, and is not skipped.
	end, err := f.eventStore.LastStoredEventID(ctx)
	if err != nil {
		return 0, err
	}
	first, err := f.eventStore.FirstStoredEventIDSince(ctx, origin.Time())
	if err != nil {
		return 0, err
	}
	if first == 0 {
		return end, nil
	}
	return first - 1, nil
}

// StoreFeed implements the Feed interface.
var _ Feed = &StoreFeed{}
