package middleware_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/event/middleware"
	"github.com/grandper/go-eventide/internal/fixture"
)

type testSubscriber struct {
	eventOccurred bool
}

func (ts *testSubscriber) Handle(context.Context, event.Event) error {
	ts.eventOccurred = true
	return nil
}

func TestMiddleware(t *testing.T) {
	t.Run("should not trigger the subscriber if there is no publisher", func(t *testing.T) {
		s := &testSubscriber{}

		r := mux.NewRouter()
		// r.Use(middleware.Publisher) -> No middleware!
		r.HandleFunc("/", func(_ http.ResponseWriter, r *http.Request) {
			event.Subscribe(r.Context(), s)
			event.Publish(r.Context(), fixture.NewSomethingHappened())
		})

		req, err := http.NewRequest(http.MethodGet, "/", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		assert.False(t, s.eventOccurred)
	})

	t.Run("should create a publisher in the context", func(t *testing.T) {
		s := &testSubscriber{}

		r := mux.NewRouter()
		r.Use(middleware.Publisher)
		r.HandleFunc("/", func(_ http.ResponseWriter, r *http.Request) {
			event.Subscribe(r.Context(), s)
			event.Publish(r.Context(), fixture.NewSomethingHappened())
		})

		req, err := http.NewRequest(http.MethodGet, "/", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		assert.True(t, s.eventOccurred)
	})

	t.Run("should create a publisher with a subscriber in the context", func(t *testing.T) {
		s := &testSubscriber{}

		r := mux.NewRouter()
		r.Use(middleware.PublisherWithSubscribers(s))
		r.HandleFunc("/", func(_ http.ResponseWriter, r *http.Request) {
			event.Publish(r.Context(), fixture.NewSomethingHappened())
		})

		req, err := http.NewRequest(http.MethodGet, "/", nil)
		require.NoError(t, err)

		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		assert.True(t, s.eventOccurred)
	})
}
