package event

import "context"

// MessageListener receives messages from a message broker. Implement it for
// the broker of your choice.
type MessageListener interface {
	// ListenToMessages listens to the messages of the given topics and calls
	// messageHandler for each of them, with a context derived from ctx for
	// that message.
	ListenToMessages(ctx context.Context, topicNames []string, messageHandler MessageHandlerFunc) error
}
