package feed

import (
	"context"
	"fmt"
	"time"

	"github.com/grandper/go-eventide/event"
)

// Relay pushes the events of a feed to a message broker (the outbox pattern):
// it publishes the events stored after its position and remembers how far it
// went. Each message carries a stored event as it is: its type as routing key
// and its body as payload, which is what an event.Forwarder publishes, so the
// receiving service reads it with an event.Listener.
//
// Delivery is at least once: a run interrupted before its position is saved
// publishes the same events again, so the receiving services must be
// idempotent.
type Relay struct {
	follower  *follower
	publisher event.MessagePublisher
}

// NewRelay creates a relay of the source to the message publisher. The
// position store remembers how far the relay went.
func NewRelay(
	source Feed,
	positions PositionStore,
	publisher event.MessagePublisher,
	options ...Option,
) *Relay {
	return &Relay{
		follower:  newFollower(source, positions, options),
		publisher: publisher,
	}
}

// PublishPendingEvents publishes the events stored after the position of the
// relay, until the end of the feed.
//
// It stops and returns the error when an event cannot be published: the
// position stays before that event, which is published again by the next run.
// The position is saved after each batch, and before returning an error.
func (r *Relay) PublishPendingEvents(ctx context.Context) error {
	if err := r.follower.catchUp(ctx, r.publish); err != nil {
		return fmt.Errorf("failed to publish the pending events: %w", err)
	}
	return nil
}

// Follow starts a goroutine that publishes the pending events on every
// interval, until the returned stop function is called or the context is
// done. A failed run is logged and retried on the next interval.
//
// The stop function returns once the goroutine has exited, so no event is
// published after it returns; it therefore waits for a run in progress to
// complete. Cancel the context to bound that wait. It is safe to call the stop
// function more than once, and after the context is done.
//
// The first run comes once the interval has elapsed: call
// PublishPendingEvents first to publish right away. Follow fails with
// ErrInvalidInterval, and follows nothing, when the interval is not positive;
// the stop function is then a no-op, so it can be deferred before the error
// is checked.
func (r *Relay) Follow(ctx context.Context, interval time.Duration) (func(), error) {
	return r.follower.follow(ctx, interval, r.PublishPendingEvents, "failed to relay the feed")
}

func (r *Relay) publish(ctx context.Context, storedEvent *StoredEvent) error {
	if err := r.publisher.Publish(ctx, storedEvent.Type(), storedEvent.Body()); err != nil {
		return fmt.Errorf("failed to publish the event %d of type '%s': %w", storedEvent.ID(), storedEvent.Type(), err)
	}
	return nil
}
