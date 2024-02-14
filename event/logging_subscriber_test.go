package event_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/internal/fixture"
)

func TestLoggingSubscriber(t *testing.T) {
	ctx := context.Background()

	t.Run("should handle the event without error", func(t *testing.T) {
		subscriber := event.NewLoggingSubscriber()
		require.NoError(t, subscriber.Handle(ctx, fixture.NewSomethingHappened()))
	})

	t.Run("should log the name of the event", func(t *testing.T) {
		var buf bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		defer slog.SetDefault(previous)

		// The subscriber captures the default logger on creation, so build it
		// after redirecting the default logger to the buffer.
		subscriber := event.NewLoggingSubscriber()
		somethingHappened := fixture.NewSomethingHappened()
		require.NoError(t, subscriber.Handle(ctx, somethingHappened))
		assert.Contains(t, buf.String(), event.Name(somethingHappened))
	})

	t.Run("should hand the context to the logger", func(t *testing.T) {
		var buf bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(slog.New(&contextValueHandler{Handler: slog.NewTextHandler(&buf, nil)}))
		defer slog.SetDefault(previous)

		subscriber := event.NewLoggingSubscriber()
		loggingCtx := context.WithValue(ctx, testContextKey{}, "value")
		require.NoError(t, subscriber.Handle(loggingCtx, fixture.NewSomethingHappened()))
		assert.Contains(t, buf.String(), "ctx=value")
	})
}

// contextValueHandler is a slog handler that adds the test value carried by
// the context to every record.
type contextValueHandler struct {
	slog.Handler
}

func (h *contextValueHandler) Handle(ctx context.Context, r slog.Record) error {
	if v, ok := ctx.Value(testContextKey{}).(string); ok {
		r.AddAttrs(slog.String("ctx", v))
	}
	return h.Handler.Handle(ctx, r)
}
