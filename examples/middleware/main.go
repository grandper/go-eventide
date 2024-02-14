package main

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/event/middleware"
)

// SomethingHappened is a domain event published on every request.
type SomethingHappened struct {
	occurredOn time.Time
}

// NewSomethingHappened creates a new SomethingHappened.
func NewSomethingHappened() *SomethingHappened {
	return &SomethingHappened{occurredOn: time.Now()}
}

// OccurredOn reports when the event happened, satisfying event.Event.
func (e *SomethingHappened) OccurredOn() time.Time {
	return e.occurredOn
}

func main() {
	r := mux.NewRouter()

	// Every request gets its own event scope with a LoggingSubscriber attached,
	// so each published event is logged.
	r.Use(middleware.PublisherWithSubscribers(event.NewLoggingSubscriber()))

	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// The handler just publishes a domain event; the middleware decides
		// who observes it.
		if err := event.Publish(r.Context(), NewSomethingHappened()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write([]byte("event published\n"))
	})

	log.Println("listening on http://localhost:8080 (GET / to publish an event)")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}
