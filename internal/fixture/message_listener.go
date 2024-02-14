package fixture

import (
	"context"

	"github.com/grandper/go-eventide/event"
)

// IncomingMessage is a message delivered to the handler by MessageListener.
type IncomingMessage struct {
	// Key is the message key handed over to the handler.
	Key string
	// Body is the message payload handed over to the handler.
	Body []byte
}

// MessageListener is a test double that delivers preset messages to the
// handler passed to ListenToMessages. It records the topics it was asked to
// listen to and the error returned by the handler for each message.
type MessageListener struct {
	// Messages are delivered to the handler, in order, on ListenToMessages.
	Messages []IncomingMessage

	// MessageContext, when set, derives the context handed over with each
	// message from the listening context. The listening context is handed
	// over as is otherwise.
	MessageContext func(ctx context.Context, m IncomingMessage) context.Context

	// Topics captures the topics passed to ListenToMessages.
	Topics []string
	// HandlerErrs captures the handler's return value for each message.
	HandlerErrs []error
}

// ListenToMessages delivers the preset messages to the handler and records the
// topics and the handler's results.
func (l *MessageListener) ListenToMessages(
	ctx context.Context,
	topics []string,
	handler event.MessageHandlerFunc,
) error {
	l.Topics = topics
	for _, m := range l.Messages {
		messageCtx := ctx
		if l.MessageContext != nil {
			messageCtx = l.MessageContext(ctx, m)
		}
		l.HandlerErrs = append(l.HandlerErrs, handler(messageCtx, m.Key, m.Body))
	}
	return nil
}

// MessageListener implements the event.MessageListener interface.
var _ event.MessageListener = &MessageListener{}
