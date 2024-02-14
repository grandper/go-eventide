package event

import (
	"context"
	"log/slog"
)

// LoggingSubscriber logs the name of each event it receives.
type LoggingSubscriber struct {
	logger *slog.Logger
}

// NewLoggingSubscriber creates a new logging subscriber that writes to the
// default slog logger.
func NewLoggingSubscriber() *LoggingSubscriber {
	return &LoggingSubscriber{
		logger: slog.Default(),
	}
}

// Handle logs the name of the event. The context is handed to the logger, so
// a context-aware slog handler can enrich the record with, for instance, a
// trace or request id.
func (hs *LoggingSubscriber) Handle(ctx context.Context, e Event) error {
	hs.logger.InfoContext(ctx, "event published", slog.String("event", Name(e)))
	return nil
}
