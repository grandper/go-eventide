package middleware

import (
	"context"
	"net/http"

	"github.com/grandper/go-eventide/event"
)

// Publisher is a middleware that opens a private event scope for the duration
// of each request, so the handler can publish through the request context.
func Publisher(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		event.PrivateScopeWithContext(r.Context(), func(ctx context.Context) {
			newRequest := r.WithContext(ctx)
			h.ServeHTTP(w, newRequest)
		})
	})
}

// PublisherWithSubscribers returns a middleware that opens a private event
// scope for the duration of each request, with the provided subscribers
// already registered.
func PublisherWithSubscribers(subscribers ...event.Subscriber) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			event.PrivateScopeWithContext(r.Context(), func(ctx context.Context) {
				newRequest := r.WithContext(ctx)
				h.ServeHTTP(w, newRequest)
			}, subscribers...)
		})
	}
}
