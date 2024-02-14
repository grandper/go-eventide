package feedhttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-serializer/serializer"

	"github.com/grandper/go-eventide/feed"
	"github.com/grandper/go-eventide/feed/feedhttp"
	"github.com/grandper/go-eventide/feed/inmemory"
	"github.com/grandper/go-eventide/internal/fixture"
)

func TestFeedController(t *testing.T) {
	occurredOn := time.Date(2026, time.September, 26, 10, 0, 0, 0, time.UTC)
	event1 := feed.NewStoredEvent(1, "SomethingHappened", occurredOn, []byte(`{"n":1}`))
	event2 := feed.NewStoredEvent(2, "SomethingHappened", occurredOn, []byte("not json"))
	wantEvents := `"events":[` +
		`{"id":1,"type":"SomethingHappened","occurredOn":"2026-09-26T10:00:00Z","body":{"n":1}},` +
		`{"id":2,"type":"SomethingHappened","occurredOn":"2026-09-26T10:00:00Z","body":"bm90IGpzb24="}]`

	newServer := func(eventStore feed.EventStore) *TestHTTPServer {
		nc := feedhttp.NewController(feed.NewStoreFeed(eventStore), eventStore)
		return NewTestHTTPServer(nc)
	}
	get := func(server *TestHTTPServer, target string) *TestResponse {
		req, _ := http.NewRequest(http.MethodGet, target, nil)
		return server.ExecuteRequest(req)
	}

	t.Run("should return the health of the service", func(t *testing.T) {
		response := get(newServer(&fixture.EventStore{}), "/events/health")

		response.AssertStatus(t, http.StatusOK)
		response.AssertBodyString(t, `{"alive": true}`)
		response.AssertContentType(t, "application/json")
	})

	t.Run("should only answer GET and DELETE requests", func(t *testing.T) {
		eventStore := &fixture.EventStore{}

		assertOtherMethodsNotAllowed(t, newServer(eventStore), "")
		assertNothingDeleted(t, eventStore)
	})

	t.Run("should only answer GET and DELETE requests under a path prefix", func(t *testing.T) {
		eventStore := &fixture.EventStore{LastID: 45}
		nc := feedhttp.NewController(feed.NewStoreFeed(eventStore), eventStore)
		server := NewTestHTTPServerUnder("/orders", nc)

		assertOtherMethodsNotAllowed(t, server, "/orders")
		assertNothingDeleted(t, eventStore)
		get(server, "/orders/events/health").AssertStatus(t, http.StatusOK)
		get(server, "/orders/events?from=end").AssertBodyString(t, `{"events":[],"next":45,"hasMore":false}`)
		get(server, "/events").AssertStatus(t, http.StatusNotFound)
	})

	t.Run("should not route the feed pages any more", func(t *testing.T) {
		get(newServer(&fixture.EventStore{}), "/events/1,20").AssertStatus(t, http.StatusNotFound)
	})

	t.Run("should return the first events of the feed", func(t *testing.T) {
		eventStore := &fixture.EventStore{After: []*feed.StoredEvent{event1, event2}}
		server := newServer(eventStore)

		for _, target := range []string{"/events", "/events?from=start", "/events?after=0"} {
			response := get(server, target)

			response.AssertStatus(t, http.StatusOK)
			response.AssertBodyString(t, `{`+wantEvents+`,"next":2,"hasMore":false}`)
			response.AssertContentType(t, "application/json")
		}
		want := fixture.AfterCall{EventID: 0, Limit: feed.DefaultLimit + 1}
		assert.Equal(t, []fixture.AfterCall{want, want, want}, eventStore.AfterCalls)
	})

	t.Run("should return the events after a position", func(t *testing.T) {
		eventStore := &fixture.EventStore{After: []*feed.StoredEvent{event1, event2}}

		response := get(newServer(eventStore), "/events?after=40&limit=1")

		response.AssertStatus(t, http.StatusOK)
		response.AssertBodyString(t, `{"events":[`+
			`{"id":1,"type":"SomethingHappened","occurredOn":"2026-09-26T10:00:00Z","body":{"n":1}}],`+
			`"next":1,"hasMore":true}`)
		assert.Equal(t, []fixture.AfterCall{{EventID: 40, Limit: 2}}, eventStore.AfterCalls)
	})

	t.Run("should return the position of the end of the feed", func(t *testing.T) {
		eventStore := &fixture.EventStore{LastID: 45}

		response := get(newServer(eventStore), "/events?from=end")

		response.AssertStatus(t, http.StatusOK)
		response.AssertBodyString(t, `{"events":[],"next":45,"hasMore":false}`)
	})

	t.Run("should return the events since a time", func(t *testing.T) {
		eventStore := &fixture.EventStore{
			LastID:       45,
			FirstIDSince: 41,
			After:        []*feed.StoredEvent{event1, event2},
		}
		server := newServer(eventStore)

		for _, since := range []string{
			"2026-09-26T10:00:00Z",
			"2026-09-26T12%3A00%3A00%2B02%3A00",
			"2026-09-26T10:00:00.000Z",
		} {
			response := get(server, "/events?since="+since)

			response.AssertStatus(t, http.StatusOK)
			response.AssertBodyString(t, `{`+wantEvents+`,"next":2,"hasMore":false}`)
		}
		require.Len(t, eventStore.FirstIDSinceCalls, 3)
		for _, since := range eventStore.FirstIDSinceCalls {
			assert.True(t, occurredOn.Equal(since))
		}
		assert.Equal(t, fixture.AfterCall{EventID: 40, Limit: feed.DefaultLimit + 1}, eventStore.AfterCalls[0])
	})

	t.Run("should return an empty events list when there is nothing to read", func(t *testing.T) {
		response := get(newServer(&fixture.EventStore{}), "/events?after=45")

		response.AssertStatus(t, http.StatusOK)
		response.AssertBodyString(t, `{"events":[],"next":45,"hasMore":false}`)
	})

	t.Run("should accept the highest limit", func(t *testing.T) {
		response := get(newServer(&fixture.EventStore{}), "/events?limit=1000")

		response.AssertStatus(t, http.StatusOK)
	})

	t.Run("should reject an invalid request", func(t *testing.T) {
		eventStore := &fixture.EventStore{}
		server := newServer(eventStore)

		for _, query := range []string{
			"limit=", "limit=0", "limit=-1", "limit=foobar", "limit=1.5", "limit=1001",
			"after=", "after=-1", "after=foobar", "after=1.5",
			"from=", "from=middle", "from=START",
			"since=", "since=yesterday", "since=2026-09-26", "since=2026-09-26T10:00:00",
			"after=1&from=start", "after=1&since=2026-09-26T10:00:00Z", "from=end&since=2026-09-26T10:00:00Z",
		} {
			response := get(server, "/events?"+query)

			assert.True(t, response.AssertStatus(t, http.StatusBadRequest), query)
			response.AssertBodyString(t, "")
		}
		assert.Empty(t, eventStore.AfterCalls)
	})

	t.Run("should leave the CORS policy to the host application", func(t *testing.T) {
		server := newServer(&fixture.EventStore{})

		for _, path := range []string{"/events/health", "/events"} {
			response := get(server, path)

			response.AssertStatus(t, http.StatusOK)
			assert.Empty(t, response.response.Header().Get("Access-Control-Allow-Origin"))
		}
	})

	t.Run("should serve any feed", func(t *testing.T) {
		source := feedFunc(func(_ context.Context, origin feed.Origin, limit int) (*feed.Batch, error) {
			assert.Equal(t, feed.After(40), origin)
			assert.Equal(t, 2, limit)
			return feed.NewBatch([]*feed.StoredEvent{event1, event2}, 2, false), nil
		})
		server := NewTestHTTPServer(feedhttp.NewController(source, &fixture.EventStore{}))

		response := get(server, "/events?after=40&limit=2")

		response.AssertStatus(t, http.StatusOK)
		response.AssertBodyString(t, `{`+wantEvents+`,"next":2,"hasMore":false}`)
	})

	t.Run("should reject a request the feed finds invalid", func(t *testing.T) {
		for _, feedErr := range []error{feed.ErrInvalidLimit, feed.ErrInvalidPosition} {
			source := feedFunc(func(context.Context, feed.Origin, int) (*feed.Batch, error) {
				return nil, fmt.Errorf("failed to read the feed: %w", feedErr)
			})
			server := NewTestHTTPServer(feedhttp.NewController(source, &fixture.EventStore{}))

			response := get(server, "/events")

			response.AssertStatus(t, http.StatusBadRequest)
			response.AssertBodyString(t, "")
		}
	})

	t.Run("should fail and log when the events cannot be read", func(t *testing.T) {
		logs := captureLogs(t)
		eventStore := &fixture.EventStore{AfterErr: errors.New("an error occurred")}

		response := get(newServer(eventStore), "/events")

		response.AssertStatus(t, http.StatusInternalServerError)
		response.AssertBodyString(t, "")
		assert.Contains(t, logs.String(), "failed to read the feed")
	})

	t.Run("should log a batch that cannot be written", func(t *testing.T) {
		logs := captureLogs(t)
		eventStore := &fixture.EventStore{}
		nc := feedhttp.NewController(feed.NewStoreFeed(eventStore), eventStore)
		w := &failingResponseWriter{header: http.Header{}}

		req, _ := http.NewRequest(http.MethodGet, "/events", nil)
		nc.EventsHandler(w, req)

		assert.Equal(t, http.StatusOK, w.status)
		assert.Contains(t, logs.String(), "failed to write the batch response")
	})

	t.Run("should not panic when the health cannot be written", func(t *testing.T) {
		w := &failingResponseWriter{header: http.Header{}}

		req, _ := http.NewRequest(http.MethodGet, "/events/health", nil)
		feedhttp.HealthCheckHandler(w, req)

		assert.Equal(t, http.StatusOK, w.status)
	})
}

func TestFeedControllerDeletion(t *testing.T) {
	occurredOn := time.Date(2026, time.September, 26, 10, 0, 0, 0, time.UTC)

	newServer := func(eventStore feed.EventStore) *TestHTTPServer {
		nc := feedhttp.NewController(feed.NewStoreFeed(eventStore), eventStore)
		return NewTestHTTPServer(nc)
	}
	get := func(server *TestHTTPServer, target string) *TestResponse {
		req, _ := http.NewRequest(http.MethodGet, target, nil)
		return server.ExecuteRequest(req)
	}
	del := func(server *TestHTTPServer, target string) *TestResponse {
		req, _ := http.NewRequest(http.MethodDelete, target, nil)
		return server.ExecuteRequest(req)
	}

	t.Run("should delete every event", func(t *testing.T) {
		eventStore := &fixture.EventStore{}

		response := del(newServer(eventStore), "/events?all=true")

		response.AssertStatus(t, http.StatusNoContent)
		response.AssertBodyString(t, "")
		assert.Equal(t, 1, eventStore.DeleteAllCalls)
		assert.Empty(t, eventStore.KeepLastCalls)
		assert.Empty(t, eventStore.DeleteBeforeCalls)
		assert.Empty(t, eventStore.DeleteOccurredBeforeCalls)
	})

	t.Run("should delete every event but the last ones", func(t *testing.T) {
		eventStore := &fixture.EventStore{}
		server := newServer(eventStore)

		for _, keep := range []string{"1", "10", "100000"} {
			response := del(server, "/events?keep="+keep)

			response.AssertStatus(t, http.StatusNoContent)
			response.AssertBodyString(t, "")
		}
		assert.Equal(t, []int{1, 10, 100000}, eventStore.KeepLastCalls)
		assert.Zero(t, eventStore.DeleteAllCalls)
		assert.Empty(t, eventStore.DeleteBeforeCalls)
		assert.Empty(t, eventStore.DeleteOccurredBeforeCalls)
	})

	t.Run("should delete the events before an id", func(t *testing.T) {
		eventStore := &fixture.EventStore{}
		server := newServer(eventStore)

		for _, before := range []string{"0", "21", "9223372036854775807"} {
			response := del(server, "/events?before="+before)

			response.AssertStatus(t, http.StatusNoContent)
			response.AssertBodyString(t, "")
		}
		assert.Equal(t, []int64{0, 21, 9223372036854775807}, eventStore.DeleteBeforeCalls)
		assert.Zero(t, eventStore.DeleteAllCalls)
		assert.Empty(t, eventStore.KeepLastCalls)
		assert.Empty(t, eventStore.DeleteOccurredBeforeCalls)
	})

	t.Run("should delete the events that occurred before a time", func(t *testing.T) {
		eventStore := &fixture.EventStore{}
		server := newServer(eventStore)

		for _, before := range []string{
			"2026-09-26T10:00:00Z",
			"2026-09-26T12%3A00%3A00%2B02%3A00",
			"2026-09-26T10:00:00.000Z",
		} {
			response := del(server, "/events?occurredBefore="+before)

			response.AssertStatus(t, http.StatusNoContent)
			response.AssertBodyString(t, "")
		}
		require.Len(t, eventStore.DeleteOccurredBeforeCalls, 3)
		for _, before := range eventStore.DeleteOccurredBeforeCalls {
			assert.True(t, occurredOn.Equal(before))
		}
		assert.Zero(t, eventStore.DeleteAllCalls)
		assert.Empty(t, eventStore.KeepLastCalls)
		assert.Empty(t, eventStore.DeleteBeforeCalls)
	})

	t.Run("should delete the events of the store, whatever the feed served", func(t *testing.T) {
		source := feedFunc(func(context.Context, feed.Origin, int) (*feed.Batch, error) {
			t.Error("the feed must not be read to delete events")
			return feed.NewBatch(nil, 0, false), nil
		})
		eventStore := &fixture.EventStore{}
		server := NewTestHTTPServer(feedhttp.NewController(source, eventStore))

		del(server, "/events?before=21").AssertStatus(t, http.StatusNoContent)

		assert.Equal(t, []int64{21}, eventStore.DeleteBeforeCalls)
	})

	t.Run("should not delete anything when reading the feed", func(t *testing.T) {
		eventStore := &fixture.EventStore{}
		server := newServer(eventStore)

		for _, query := range []string{"all=true", "keep=10", "before=21", "occurredBefore=2026-09-26T10:00:00Z"} {
			get(server, "/events?"+query).AssertStatus(t, http.StatusOK)
		}
		assertNothingDeleted(t, eventStore)
	})

	t.Run("should serve a feed without the deleted events", func(t *testing.T) {
		eventStore := inmemory.NewEventStore(serializer.NewJSONSerializer())
		for range 5 {
			require.NoError(t, eventStore.Append(context.Background(), fixture.NewSomethingHappened()))
		}
		server := newServer(eventStore)
		ids := func() string {
			response := get(server, "/events")
			response.AssertStatus(t, http.StatusOK)
			var batch struct {
				Events []struct {
					ID int64 `json:"id"`
				} `json:"events"`
			}
			require.NoError(t, json.Unmarshal(response.response.Body.Bytes(), &batch))
			return fmt.Sprint(batch.Events)
		}

		del(server, "/events?before=2").AssertStatus(t, http.StatusNoContent)
		assert.Equal(t, "[{2} {3} {4} {5}]", ids())

		del(server, "/events?keep=2").AssertStatus(t, http.StatusNoContent)
		assert.Equal(t, "[{4} {5}]", ids())

		del(server, "/events?all=true").AssertStatus(t, http.StatusNoContent)
		assert.Equal(t, "[]", ids())

		require.NoError(t, eventStore.Append(context.Background(), fixture.NewSomethingHappened()))
		assert.Equal(t, "[{6}]", ids())

		del(server, "/events?occurredBefore=2030-01-01T00:00:00Z").AssertStatus(t, http.StatusNoContent)
		assert.Equal(t, "[]", ids())
	})

	t.Run("should reject an invalid deletion", func(t *testing.T) {
		eventStore := &fixture.EventStore{}
		server := newServer(eventStore)

		for _, target := range []string{
			"/events", "/events?", "/events?limit=10", "/events?after=21", "/events?keepLast=10",
			"/events?all=", "/events?all", "/events?all=false", "/events?all=TRUE", "/events?all=1", "/events?all=yes",
			"/events?keep=", "/events?keep=0", "/events?keep=-1", "/events?keep=foobar", "/events?keep=1.5",
			"/events?keep=9223372036854775808",
			"/events?before=", "/events?before=-1", "/events?before=foobar", "/events?before=1.5",
			"/events?before=9223372036854775808",
			"/events?occurredBefore=", "/events?occurredBefore=yesterday", "/events?occurredBefore=2026-09-26",
			"/events?occurredBefore=2026-09-26T10:00:00",
			"/events?all=true&keep=10", "/events?all=true&before=21", "/events?keep=10&before=21",
			"/events?all=true&occurredBefore=2026-09-26T10:00:00Z",
			"/events?before=21&occurredBefore=2026-09-26T10:00:00Z",
			"/events?keep=10&occurredBefore=2026-09-26T10:00:00Z",
			"/events?all=true&keep=10&before=21&occurredBefore=2026-09-26T10:00:00Z",
			"/events?all=true&all=true", "/events?keep=10&keep=20", "/events?before=21&before=42",
			"/events?all=true&keep=foobar", "/events?before=foobar&all=true",
		} {
			response := del(server, target)

			assert.True(t, response.AssertStatus(t, http.StatusBadRequest), target)
			response.AssertBodyString(t, "")
		}
		assertNothingDeleted(t, eventStore)
	})

	t.Run("should fail and log when the events cannot be deleted", func(t *testing.T) {
		for _, query := range []string{"all=true", "keep=10", "before=21", "occurredBefore=2026-09-26T10:00:00Z"} {
			logs := captureLogs(t)
			eventStore := &fixture.EventStore{DeleteErr: errors.New("an error occurred")}

			response := del(newServer(eventStore), "/events?"+query)

			assert.True(t, response.AssertStatus(t, http.StatusInternalServerError), query)
			response.AssertBodyString(t, "")
			assert.Contains(t, logs.String(), "failed to delete the events")
			assert.Contains(t, logs.String(), "an error occurred")
		}
	})

	t.Run("should delete the events with the context of the request", func(t *testing.T) {
		eventStore := &contextRecordingEventStore{}
		server := NewTestHTTPServer(feedhttp.NewController(feed.NewStoreFeed(eventStore), eventStore))
		type key struct{}
		ctx := context.WithValue(context.Background(), key{}, "request")

		for _, query := range []string{"all=true", "keep=10", "before=21", "occurredBefore=2026-09-26T10:00:00Z"} {
			req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, "/events?"+query, nil)
			server.ExecuteRequest(req).AssertStatus(t, http.StatusNoContent)
		}

		require.Len(t, eventStore.contexts, 4)
		for _, deletionCtx := range eventStore.contexts {
			assert.Equal(t, "request", deletionCtx.Value(key{}))
		}
	})
}

// feedFunc is a function used as a feed.
type feedFunc func(ctx context.Context, origin feed.Origin, limit int) (*feed.Batch, error)

func (f feedFunc) RetrieveEvents(ctx context.Context, origin feed.Origin, limit int) (*feed.Batch, error) {
	return f(ctx, origin, limit)
}

// failingResponseWriter is a response writer whose body cannot be written. It
// records the first status code it is given, as a real response would.
type failingResponseWriter struct {
	header http.Header
	status int
}

func (w *failingResponseWriter) Header() http.Header { return w.header }

func (w *failingResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("an error occurred")
}

func (w *failingResponseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

// syncBuffer is a buffer that can be written by a goroutine while the test
// reads it.
type syncBuffer struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.buffer.String()
}

// captureLogs redirects the default logger, used by the controllers created
// afterwards, to the returned buffer until the end of the test.
func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	logs := &syncBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return logs
}

// assertNothingDeleted asserts that no deletion reached the event store.
// assertOtherMethodsNotAllowed asserts that the routes of the feed, mounted
// under the path prefix, answer a "405 Method Not Allowed" naming the methods
// they accept to the requests of any other method.
func assertOtherMethodsNotAllowed(t *testing.T, server *TestHTTPServer, pathPrefix string) {
	t.Helper()
	for _, tc := range []struct{ method, target, allow string }{
		{http.MethodPost, "/events/health", "GET"},
		{http.MethodDelete, "/events/health", "GET"},
		{http.MethodHead, "/events/health", "GET"},
		{http.MethodPost, "/events", "GET, DELETE"},
		{http.MethodPut, "/events", "GET, DELETE"},
		{http.MethodPatch, "/events", "GET, DELETE"},
		{http.MethodPost, "/events?all=true", "GET, DELETE"},
	} {
		req, _ := http.NewRequest(tc.method, pathPrefix+tc.target, nil)
		response := server.ExecuteRequest(req)

		assert.True(t, response.AssertStatus(t, http.StatusMethodNotAllowed), tc.method+" "+tc.target)
		assert.True(t, response.AssertHeader(t, "Allow", tc.allow), tc.method+" "+tc.target)
	}
}

func assertNothingDeleted(t *testing.T, eventStore *fixture.EventStore) {
	t.Helper()
	assert.Zero(t, eventStore.DeleteAllCalls)
	assert.Empty(t, eventStore.KeepLastCalls)
	assert.Empty(t, eventStore.DeleteBeforeCalls)
	assert.Empty(t, eventStore.DeleteOccurredBeforeCalls)
}

// contextRecordingEventStore is an event store recording the context of the
// deletions it is asked.
type contextRecordingEventStore struct {
	fixture.EventStore

	contexts []context.Context
}

func (s *contextRecordingEventStore) DeleteAllStoredEvents(ctx context.Context) error {
	s.contexts = append(s.contexts, ctx)
	return nil
}

func (s *contextRecordingEventStore) KeepLastStoredEvents(ctx context.Context, _ int) error {
	s.contexts = append(s.contexts, ctx)
	return nil
}

func (s *contextRecordingEventStore) DeleteStoredEventsBefore(ctx context.Context, _ int64) error {
	s.contexts = append(s.contexts, ctx)
	return nil
}

func (s *contextRecordingEventStore) DeleteStoredEventsOccurredBefore(ctx context.Context, _ time.Time) error {
	s.contexts = append(s.contexts, ctx)
	return nil
}
