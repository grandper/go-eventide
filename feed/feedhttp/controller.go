package feedhttp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"github.com/grandper/go-eventide/feed"
)

// Controller serves a feed over HTTP, and deletes events of the event store
// behind it.
type Controller struct {
	source     feed.Feed
	eventStore feed.EventStore
	logger     *slog.Logger
}

// NewController creates the controller serving the source over HTTP, and
// deleting the events of the event store, the one the source reads. The
// failures to serve them are logged to slog.Default().
//
// The controller does not protect its routes: deleting is open to whoever
// reaches the route, so guard it in the host application, with a middleware
// for instance.
func NewController(source feed.Feed, eventStore feed.EventStore) *Controller {
	return &Controller{
		source:     source,
		eventStore: eventStore,
		logger:     slog.Default(),
	}
}

// HealthCheckHandler answers that the service is alive.
func HealthCheckHandler(w http.ResponseWriter, _ *http.Request) {
	setHeaders(w)
	w.WriteHeader(http.StatusOK)
	_, err := io.WriteString(w, `{"alive": true}`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// EventsHandler writes a batch of events of the feed as JSON. The reading
// begins at the origin given by one of the "after", "from" and "since" query
// parameters, and at the start of the feed when none is given. The "limit"
// query parameter bounds the number of events, and is feed.DefaultLimit when
// absent.
//
// It answers a "400 Bad Request" when more than one origin is given, or when a
// parameter is invalid: "after" is not an integer >= 0, "from" is not "start"
// or "end", "since" is not an RFC 3339 time, "limit" is not an integer between
// 1 and feed.MaxLimit. It answers a "500 Internal Server Error", and logs the
// error, when the feed cannot be read.
func (c *Controller) EventsHandler(w http.ResponseWriter, r *http.Request) {
	setHeaders(w)
	query := r.URL.Query()
	origin, ok := originFromQuery(query)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	limit, ok := limitFromQuery(query)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	batch, err := c.source.RetrieveEvents(r.Context(), origin, limit)
	if errors.Is(err, feed.ErrInvalidLimit) || errors.Is(err, feed.ErrInvalidPosition) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err != nil {
		c.logger.ErrorContext(r.Context(), "failed to read the feed", slog.Any("error", err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	c.writeBatch(r.Context(), w, batch)
}

// DeleteEventsHandler deletes events of the event store. What is deleted is
// given by exactly one query parameter: "all" deletes every event, "keep"
// deletes every event but that number of last ones, "before" deletes the
// events whose id is lower, and "occurredBefore" deletes the events that
// occurred before that time. It answers a "204 No Content" when the deletion
// succeeded, whether events were deleted or not.
//
// It answers a "400 Bad Request", and deletes nothing, when none or more than
// one of these parameters is given, or when the parameter is invalid: "all"
// is not "true", "keep" is not an integer >= 1, "before" is not an integer
// >= 0, "occurredBefore" is not an RFC 3339 time. It answers a "500 Internal
// Server Error", and logs the error, when the events cannot be deleted.
func (c *Controller) DeleteEventsHandler(w http.ResponseWriter, r *http.Request) {
	deleteEvents, ok := deletionFromQuery(r.URL.Query())
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := deleteEvents(r.Context(), c.eventStore); err != nil {
		c.logger.ErrorContext(r.Context(), "failed to delete the events", slog.Any("error", err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (c *Controller) writeBatch(ctx context.Context, w http.ResponseWriter, batch *feed.Batch) {
	events := make([]storedEventResponse, 0, len(batch.Events()))
	for _, e := range batch.Events() {
		events = append(events, newStoredEventResponse(e))
	}
	b, err := json.Marshal(batchResponse{
		Events:  events,
		Next:    batch.Next(),
		HasMore: batch.HasMore(),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, writeErr := w.Write(b); writeErr != nil {
		c.logger.ErrorContext(ctx, "failed to write the batch response", slog.Any("error", writeErr))
	}
}

// batchResponse is the JSON representation of a batch.
type batchResponse struct {
	Events  []storedEventResponse `json:"events"`
	Next    int64                 `json:"next"`
	HasMore bool                  `json:"hasMore"`
}

// storedEventResponse is the JSON representation of a stored event.
type storedEventResponse struct {
	ID         int64           `json:"id"`
	Type       string          `json:"type"`
	OccurredOn time.Time       `json:"occurredOn"`
	Body       json.RawMessage `json:"body"`
}

func newStoredEventResponse(e *feed.StoredEvent) storedEventResponse {
	return storedEventResponse{
		ID:         e.ID(),
		Type:       e.Type(),
		OccurredOn: e.OccurredOn(),
		Body:       bodyToJSON(e.Body()),
	}
}

// bodyToJSON embeds the event body as-is when it is valid JSON (the default
// serializer produces JSON); any other payload is carried as a base64 string.
func bodyToJSON(body []byte) json.RawMessage {
	if json.Valid(body) {
		return json.RawMessage(body)
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return json.RawMessage("null")
	}
	return encoded
}

// originFromQuery returns the origin requested in the query, and false when
// more than one origin is given or when the origin is invalid.
func originFromQuery(query url.Values) (feed.Origin, bool) {
	given := 0
	for _, param := range []string{AfterParam, FromParam, SinceParam} {
		if query.Has(param) {
			given++
		}
	}
	switch {
	case given > 1:
		return feed.Origin{}, false
	case query.Has(AfterParam):
		position, err := strconv.ParseInt(query.Get(AfterParam), 10, 64)
		if err != nil || position < 0 {
			return feed.Origin{}, false
		}
		return feed.After(position), true
	case query.Has(FromParam):
		return originFrom(query.Get(FromParam))
	case query.Has(SinceParam):
		since, err := time.Parse(time.RFC3339, query.Get(SinceParam))
		if err != nil {
			return feed.Origin{}, false
		}
		return feed.Since(since), true
	default:
		return feed.FromStart(), true
	}
}

func originFrom(from string) (feed.Origin, bool) {
	switch from {
	case FromStart:
		return feed.FromStart(), true
	case FromEnd:
		return feed.FromEnd(), true
	default:
		return feed.Origin{}, false
	}
}

// deletion deletes events of an event store.
type deletion func(ctx context.Context, eventStore feed.EventStore) error

// deletionFromQuery returns the deletion requested in the query, and false
// when none or more than one deletion is given or when the deletion is
// invalid. A parameter given twice counts as two deletions.
func deletionFromQuery(query url.Values) (deletion, bool) {
	given := 0
	for _, param := range []string{AllParam, KeepParam, BeforeParam, OccurredBeforeParam} {
		given += len(query[param])
	}
	switch {
	case given != 1:
		return nil, false
	case query.Has(AllParam):
		if query.Get(AllParam) != AllTrue {
			return nil, false
		}
		return func(ctx context.Context, eventStore feed.EventStore) error {
			return eventStore.DeleteAllStoredEvents(ctx)
		}, true
	case query.Has(KeepParam):
		count, err := strconv.Atoi(query.Get(KeepParam))
		if err != nil || count < 1 {
			return nil, false
		}
		return func(ctx context.Context, eventStore feed.EventStore) error {
			return eventStore.KeepLastStoredEvents(ctx, count)
		}, true
	case query.Has(BeforeParam):
		eventID, err := strconv.ParseInt(query.Get(BeforeParam), 10, 64)
		if err != nil || eventID < 0 {
			return nil, false
		}
		return func(ctx context.Context, eventStore feed.EventStore) error {
			return eventStore.DeleteStoredEventsBefore(ctx, eventID)
		}, true
	default:
		before, err := time.Parse(time.RFC3339, query.Get(OccurredBeforeParam))
		if err != nil {
			return nil, false
		}
		return func(ctx context.Context, eventStore feed.EventStore) error {
			return eventStore.DeleteStoredEventsOccurredBefore(ctx, before)
		}, true
	}
}

// limitFromQuery returns the limit requested in the query, and false when it
// is not an integer between 1 and feed.MaxLimit.
func limitFromQuery(query url.Values) (int, bool) {
	if !query.Has(LimitParam) {
		return feed.DefaultLimit, true
	}
	limit, err := strconv.Atoi(query.Get(LimitParam))
	if err != nil || limit < 1 || limit > feed.MaxLimit {
		return 0, false
	}
	return limit, true
}

func setHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}

// Register mounts the feed handlers on the given router under the "/events"
// path. The events are read with GET requests and deleted with DELETE
// requests; the health check only accepts GET requests.
//
// The feed owns its two paths, "/events" and "/events/health": a request of
// any other method answers a "405 Method Not Allowed" naming the allowed
// methods in the "Allow" header, whether the router is the root one or a
// sub-router mounted under a path prefix.
func (c *Controller) Register(r *mux.Router) {
	routes(r)
	r.Get(HealthRoute).HandlerFunc(HealthCheckHandler)
	r.Get(EventsRoute).HandlerFunc(c.EventsHandler)
	r.Get(DeleteEventsRoute).HandlerFunc(c.DeleteEventsHandler)
	otherMethods(r)
}
