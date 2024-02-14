package fixture

import (
	"context"
	"sync"

	"github.com/grandper/go-eventide/event"
)

// PublishedMessage is a message recorded by MessagePublisher.
type PublishedMessage struct {
	// Key is the key the message was published with.
	Key string
	// Message is the published payload.
	Message []byte
}

// MessagePublisher is a test double that records the messages it publishes.
// Set Err to make Publish fail without recording the message.
type MessagePublisher struct {
	mutex     sync.Mutex
	published []PublishedMessage

	// Err is returned by Publish when non-nil.
	Err error
}

// Publish records the message, or returns Err when it is set.
func (p *MessagePublisher) Publish(_ context.Context, key string, message []byte) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.Err != nil {
		return p.Err
	}
	p.published = append(p.published, PublishedMessage{Key: key, Message: message})
	return nil
}

// Published returns the messages that have been published, in order.
func (p *MessagePublisher) Published() []PublishedMessage {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return append([]PublishedMessage(nil), p.published...)
}

// MessagePublisher implements the event.MessagePublisher interface.
var _ event.MessagePublisher = &MessagePublisher{}
