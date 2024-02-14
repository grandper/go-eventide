package event

import "context"

// MessagePublisher publishes messages on a message broker. Implement it for
// the broker of your choice.
type MessagePublisher interface {
	// Publish publishes a message under a routing key.
	Publish(ctx context.Context, key string, message []byte) error
}
