package feed

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/grandper/go-eventide/event"
)

// Consumer catches up with a feed: it reads the events stored after its
// position, hands them to its subscribers and remembers how far it went.
//
// Delivery is at least once: a run interrupted before its position is saved
// hands the same events again, so the subscribers must be idempotent.
type Consumer struct {
	follower   *follower
	serializer event.Serializer

	mutex       sync.RWMutex
	subscribers []event.Subscriber
}

// NewConsumer creates a consumer of the source. The serializer turns the
// stored events back into events: the types of the events the consumer is
// interested in are registered on it. The position store remembers how far
// the consumer went.
func NewConsumer(
	source Feed,
	serializer event.Serializer,
	positions PositionStore,
	options ...Option,
) *Consumer {
	return &Consumer{
		follower:   newFollower(source, positions, options),
		serializer: serializer,
	}
}

// Subscribe adds a subscriber to the events of the feed.
func (c *Consumer) Subscribe(subscriber event.Subscriber) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// The subscribers are copied to a new slice rather than appended in
	// place, so the run in progress keeps reading its own slice.
	c.subscribers = append(slices.Clone(c.subscribers), subscriber)
}

// CatchUp reads the feed from the position of the consumer until its end, and
// hands each event to the subscribers, in the order they subscribed.
//
// An event whose type is not registered on the serializer is skipped. CatchUp
// stops and returns the error when an event cannot be deserialized or when a
// subscriber fails: the position stays before that event, which is handed
// again by the next run. The position is saved after each batch, and before
// returning an error.
func (c *Consumer) CatchUp(ctx context.Context) error {
	if err := c.follower.catchUp(ctx, c.handle); err != nil {
		return fmt.Errorf("failed to catch up with the feed: %w", err)
	}
	return nil
}

// Follow starts a goroutine that catches up on every interval, until the
// returned stop function is called or the context is done. A failed run is
// logged and retried on the next interval.
//
// The stop function returns once the goroutine has exited, so no event is
// handed to the subscribers after it returns; it therefore waits for a run in
// progress to complete. Cancel the context to bound that wait. It is safe to
// call the stop function more than once, and after the context is done.
//
// The first run comes once the interval has elapsed: call CatchUp first to
// catch up right away. Follow fails with ErrInvalidInterval, and follows
// nothing, when the interval is not positive; the stop function is then a
// no-op, so it can be deferred before the error is checked.
func (c *Consumer) Follow(ctx context.Context, interval time.Duration) (func(), error) {
	return c.follower.follow(ctx, interval, c.CatchUp, "failed to follow the feed")
}

// handle turns the stored event back into an event and hands it to the
// subscribers.
func (c *Consumer) handle(ctx context.Context, storedEvent *StoredEvent) error {
	e, err := event.Deserialize(c.serializer, storedEvent.Body())
	if errors.Is(err, event.ErrTypeNotRegistered) {
		// The consumer registers the events it is interested in, not all
		// the events of the feed.
		return nil
	}
	if err == nil {
		err = c.handOver(ctx, e)
	}
	if err != nil {
		return fmt.Errorf("failed to handle the event %d of type '%s': %w", storedEvent.ID(), storedEvent.Type(), err)
	}
	return nil
}

func (c *Consumer) handOver(ctx context.Context, e event.Event) error {
	c.mutex.RLock()
	subscribers := c.subscribers
	c.mutex.RUnlock()

	for _, subscriber := range subscribers {
		if err := subscriber.Handle(ctx, e); err != nil {
			return err
		}
	}
	return nil
}
