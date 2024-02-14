package feed

import (
	"context"
	"errors"
)

const (
	// DefaultLimit is the number of events of a batch when no limit is requested.
	DefaultLimit = 20
	// MaxLimit is the highest number of events of a batch.
	MaxLimit = 1000
)

var (
	// ErrInvalidPosition is returned when the position is negative.
	ErrInvalidPosition = errors.New("the feed position must not be negative")
	// ErrInvalidLimit is returned when the limit is negative or above MaxLimit.
	ErrInvalidLimit = errors.New("the feed limit must be between 1 and MaxLimit")
	// ErrInvalidInterval is returned when the interval to follow a feed at is
	// not positive.
	ErrInvalidInterval = errors.New("the interval to follow the feed at must be positive")
)

// Feed is a flow of stored events that can be read from an origin.
type Feed interface {
	// RetrieveEvents returns at most limit events from the origin. A limit of 0
	// stands for DefaultLimit. It fails with ErrInvalidLimit when the limit
	// is negative or above MaxLimit, and with ErrInvalidPosition when the
	// origin is a negative position.
	RetrieveEvents(ctx context.Context, origin Origin, limit int) (*Batch, error)
}

// NormalizeLimit returns the number of events to read for a requested limit:
// DefaultLimit when it is 0. It fails with ErrInvalidLimit when the limit is
// negative or above MaxLimit.
func NormalizeLimit(limit int) (int, error) {
	switch {
	case limit < 0 || limit > MaxLimit:
		return 0, ErrInvalidLimit
	case limit == 0:
		return DefaultLimit, nil
	default:
		return limit, nil
	}
}
