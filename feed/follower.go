package feed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// handleFunc handles a stored event of the feed.
type handleFunc func(ctx context.Context, storedEvent *StoredEvent) error

// follower reads a feed from the position it saved, batch after batch. It is
// what a Consumer and a Relay have in common.
type follower struct {
	source    Feed
	positions PositionStore
	settings  settings

	// running makes the runs follow one another.
	running sync.Mutex
}

func newFollower(source Feed, positions PositionStore, options []Option) *follower {
	return &follower{
		source:    source,
		positions: positions,
		settings:  newSettings(options),
	}
}

// catchUp reads the feed from the saved position until its end, and hands
// each event to handle. It stops when handle fails. The position is saved
// after each batch, and before returning the error of handle.
func (f *follower) catchUp(ctx context.Context, handle handleFunc) error {
	f.running.Lock()
	defer f.running.Unlock()

	origin, err := f.currentOrigin(ctx)
	if err != nil {
		return err
	}
	for {
		batch, readErr := f.source.RetrieveEvents(ctx, origin, f.settings.limit)
		if readErr != nil {
			return readErr
		}
		position, handleErr := handleBatch(ctx, batch, handle)
		if saveErr := f.positions.SavePosition(ctx, position); saveErr != nil {
			handleErr = errors.Join(handleErr, saveErr)
		}
		if handleErr != nil {
			return handleErr
		}
		if !batch.HasMore() {
			return nil
		}
		origin = After(position)
	}
}

// follow starts a goroutine that calls run on every interval, until the
// returned stop function is called or the context is done. A failed run is
// logged with the given message. The stop function returns once the goroutine
// has exited.
//
// It fails with ErrInvalidInterval, without starting a goroutine, when the
// interval is not positive. The stop function it then returns does nothing,
// so it is always safe to call.
func (f *follower) follow(
	ctx context.Context,
	interval time.Duration,
	run func(ctx context.Context) error,
	failure string,
) (func(), error) {
	if interval <= 0 {
		return func() {}, fmt.Errorf("%s: %w", failure, ErrInvalidInterval)
	}
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := run(ctx); err != nil {
					f.settings.logger.ErrorContext(ctx, failure, slog.Any("error", err))
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(done) })
		<-stopped
	}, nil
}

// currentOrigin returns where the next reading begins: after the saved
// position, or at the origin of the settings when the feed was never read.
func (f *follower) currentOrigin(ctx context.Context) (Origin, error) {
	position, found, err := f.positions.Position(ctx)
	if err != nil {
		return Origin{}, err
	}
	if !found {
		return f.settings.origin, nil
	}
	return After(position), nil
}

// handleBatch hands the events of the batch to handle. It returns the
// position reached: the one of the batch, or the one before the event that
// failed.
func handleBatch(ctx context.Context, batch *Batch, handle handleFunc) (int64, error) {
	for _, storedEvent := range batch.Events() {
		if err := handle(ctx, storedEvent); err != nil {
			// No event is visible between the previous event and this one,
			// so the position before this one is its id minus one.
			return storedEvent.ID() - 1, err
		}
	}
	return batch.Next(), nil
}
