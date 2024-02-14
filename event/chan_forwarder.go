package event

import "context"

// ChanForwarder forwards events to a caller-owned channel. The caller owns
// the channel and the forwarder never closes it. For a subscriber that owns
// its channel and supports Close, see ChanSubscriber.
type ChanForwarder struct {
	ch chan<- Event
}

// NewChanForwarder returns a new ChanForwarder.
func NewChanForwarder(ch chan<- Event) *ChanForwarder {
	return &ChanForwarder{
		ch: ch,
	}
}

// Handle forwards the event to the channel. It blocks while the channel is
// full, and gives up with the context's error when the context is done first.
func (f *ChanForwarder) Handle(ctx context.Context, e Event) error {
	select {
	case f.ch <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
