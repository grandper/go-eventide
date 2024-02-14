package feedhttp

import (
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

const (
	// eventsPath is the path of the events of the feed.
	eventsPath = "/events"
	// healthPath is the path of the health check.
	healthPath = "/events/health"
)

const (
	// EventsRoute is the name of the route of the events of the feed.
	EventsRoute = "events"
	// DeleteEventsRoute is the name of the route deleting events of the event
	// store.
	DeleteEventsRoute = "deleteEvents"
	// HealthRoute is the name of the route of the health check.
	HealthRoute = "health"
)

const (
	// AfterParam is the name of the query parameter carrying the position the
	// reading begins after.
	AfterParam = "after"
	// FromParam is the name of the query parameter carrying the end of the
	// feed the reading begins at: FromStart or FromEnd.
	FromParam = "from"
	// SinceParam is the name of the query parameter carrying the time, in the
	// RFC 3339 format, the reading begins at.
	SinceParam = "since"
	// LimitParam is the name of the query parameter carrying the highest
	// number of events to return.
	LimitParam = "limit"
)

const (
	// FromStart is the value of FromParam to read the feed from its start.
	FromStart = "start"
	// FromEnd is the value of FromParam to read the feed from its end.
	FromEnd = "end"
)

const (
	// AllParam is the name of the query parameter asking to delete every
	// event: its only value is AllTrue.
	AllParam = "all"
	// KeepParam is the name of the query parameter carrying the number of
	// last events to keep: the others are deleted.
	KeepParam = "keep"
	// BeforeParam is the name of the query parameter carrying the id the
	// deleted events are lower than.
	BeforeParam = "before"
	// OccurredBeforeParam is the name of the query parameter carrying the
	// time, in the RFC 3339 format, the deleted events occurred before.
	OccurredBeforeParam = "occurredBefore"
)

// AllTrue is the value of AllParam to delete every event.
const AllTrue = "true"

// routes declares the routes of the feed on the router. The controller
// attaches its handlers to them, and the client builds its URLs from them.
func routes(r *mux.Router) {
	r.Path(healthPath).Methods(http.MethodGet).Name(HealthRoute)
	r.Path(eventsPath).Methods(http.MethodGet).Name(EventsRoute)
	r.Path(eventsPath).Methods(http.MethodDelete).Name(DeleteEventsRoute)
}

// otherMethods declares, after the routes of the feed, the routes answering
// the requests of any other method on their paths.
//
// The router is not left to answer them: when the feed is mounted on a
// sub-router, the path prefix matcher of the routes declared afterwards makes
// it forget that a path matched, and answer a "404 Not Found".
func otherMethods(r *mux.Router) {
	r.Path(healthPath).Handler(methodNotAllowed(http.MethodGet))
	r.Path(eventsPath).Handler(methodNotAllowed(http.MethodGet, http.MethodDelete))
}

// methodNotAllowed answers a "405 Method Not Allowed" naming, in the "Allow"
// header, the methods that are allowed.
func methodNotAllowed(allowed ...string) http.HandlerFunc {
	allow := strings.Join(allowed, ", ")
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Allow", allow)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
