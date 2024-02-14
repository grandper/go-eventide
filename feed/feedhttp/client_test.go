package feedhttp_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/grandper/go-serializer/serializer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/event/eventtest"
	"github.com/grandper/go-eventide/feed"
	"github.com/grandper/go-eventide/feed/feedhttp"
	"github.com/grandper/go-eventide/feed/inmemory"
	"github.com/grandper/go-eventide/internal/fixture"
)

// recordedRequests records the requests a server receives.
type recordedRequests struct {
	requests []*http.Request
}

func (r *recordedRequests) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		r.requests = append(r.requests, request)
		next.ServeHTTP(w, request)
	})
}

func (r *recordedRequests) last() *http.Request {
	return r.requests[len(r.requests)-1]
}

// serveFeed serves the feed of the store under the path prefix, behind a
// real HTTP server, and returns its URL and the requests it receives.
func serveFeed(t *testing.T, eventStore feed.EventStore, pathPrefix string) (string, *recordedRequests) {
	t.Helper()
	recorded := &recordedRequests{}
	r := mux.NewRouter()
	feedRouter := r
	if pathPrefix != "" {
		feedRouter = r.PathPrefix(pathPrefix).Subrouter()
	}
	feedhttp.NewController(feed.NewStoreFeed(eventStore), eventStore).Register(feedRouter)
	server := httptest.NewServer(recorded.middleware(r))
	t.Cleanup(server.Close)
	return server.URL + pathPrefix, recorded
}

func TestClient(t *testing.T) {
	ctx := context.Background()
	occurredOn := time.Date(2026, time.September, 26, 10, 0, 0, 0, time.UTC)
	event1 := feed.NewStoredEvent(1, "SomethingHappened", occurredOn, []byte(`{"n":1}`))
	event2 := feed.NewStoredEvent(2, "SomethingHappened", occurredOn, []byte("not json"))

	t.Run("should fail to be created from an invalid base URL", func(t *testing.T) {
		for _, baseURL := range []string{"", "orders.example.com", "/feed", "http://%zz"} {
			client, err := feedhttp.NewClient(baseURL)
			require.Error(t, err, baseURL)
			assert.Nil(t, client)
		}
	})

	t.Run("should read the events as the feed holds them", func(t *testing.T) {
		eventStore := &fixture.EventStore{After: []*feed.StoredEvent{event1, event2}}
		baseURL, _ := serveFeed(t, eventStore, "")
		client, err := feedhttp.NewClient(baseURL)
		require.NoError(t, err)

		batch, err := client.RetrieveEvents(ctx, feed.After(40), 1)
		require.NoError(t, err)
		assert.Equal(t, []*feed.StoredEvent{event1}, batch.Events())
		assert.Equal(t, int64(1), batch.Next())
		assert.True(t, batch.HasMore())

		batch, err = client.RetrieveEvents(ctx, feed.FromStart(), 0)
		require.NoError(t, err)
		// The body that is not JSON travelled in base64.
		assert.Equal(t, []*feed.StoredEvent{event1, event2}, batch.Events())
		assert.Equal(t, int64(2), batch.Next())
		assert.False(t, batch.HasMore())
	})

	t.Run("should never return nil events", func(t *testing.T) {
		baseURL, _ := serveFeed(t, &fixture.EventStore{}, "")
		client, err := feedhttp.NewClient(baseURL)
		require.NoError(t, err)

		batch, err := client.RetrieveEvents(ctx, feed.After(45), 20)
		require.NoError(t, err)
		assert.NotNil(t, batch.Events())
		assert.Empty(t, batch.Events())
		assert.Equal(t, int64(45), batch.Next())
	})

	t.Run("should ask for the origin and the limit", func(t *testing.T) {
		eventStore := &fixture.EventStore{LastID: 45, FirstIDSince: 41}
		baseURL, recorded := serveFeed(t, eventStore, "")
		client, err := feedhttp.NewClient(baseURL)
		require.NoError(t, err)
		since := time.Date(2026, time.September, 26, 12, 0, 0, 500, time.FixedZone("CEST", 2*60*60))

		for _, tc := range []struct {
			origin feed.Origin
			limit  int
			want   url.Values
		}{
			{feed.FromStart(), 0, url.Values{"from": {"start"}, "limit": {"20"}}},
			{feed.FromEnd(), 50, url.Values{"from": {"end"}, "limit": {"50"}}},
			{feed.After(45), 1000, url.Values{"after": {"45"}, "limit": {"1000"}}},
			{feed.Since(since), 20, url.Values{"since": {"2026-09-26T10:00:00.0000005Z"}, "limit": {"20"}}},
		} {
			_, err = client.RetrieveEvents(ctx, tc.origin, tc.limit)
			require.NoError(t, err)

			request := recorded.last()
			assert.Equal(t, http.MethodGet, request.Method)
			assert.Equal(t, "/events", request.URL.Path)
			assert.Equal(t, tc.want, request.URL.Query())
		}
		require.Len(t, eventStore.FirstIDSinceCalls, 1)
		assert.True(t, since.Equal(eventStore.FirstIDSinceCalls[0]))
	})

	t.Run("should read a feed mounted under a path", func(t *testing.T) {
		eventStore := &fixture.EventStore{After: []*feed.StoredEvent{event1}}
		baseURL, recorded := serveFeed(t, eventStore, "/orders/v1")

		for _, base := range []string{baseURL, baseURL + "/"} {
			client, err := feedhttp.NewClient(base)
			require.NoError(t, err)

			batch, err := client.RetrieveEvents(ctx, feed.FromStart(), 20)
			require.NoError(t, err)
			assert.Len(t, batch.Events(), 1)
			assert.Equal(t, "/orders/v1/events", recorded.last().URL.Path)
		}
	})

	t.Run("should use the given HTTP client", func(t *testing.T) {
		baseURL, recorded := serveFeed(t, &fixture.EventStore{}, "")
		httpClient := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
			r.Header.Set("Authorization", "Bearer token")
			return http.DefaultTransport.RoundTrip(r)
		})}
		client, err := feedhttp.NewClient(baseURL, feedhttp.WithHTTPClient(httpClient))
		require.NoError(t, err)

		_, err = client.RetrieveEvents(ctx, feed.FromStart(), 20)
		require.NoError(t, err)
		assert.Equal(t, "Bearer token", recorded.last().Header.Get("Authorization"))
	})

	t.Run("should not call the feed for an invalid reading", func(t *testing.T) {
		baseURL, recorded := serveFeed(t, &fixture.EventStore{}, "")
		client, err := feedhttp.NewClient(baseURL)
		require.NoError(t, err)

		_, err = client.RetrieveEvents(ctx, feed.FromStart(), -1)
		require.ErrorIs(t, err, feed.ErrInvalidLimit)
		_, err = client.RetrieveEvents(ctx, feed.FromStart(), feed.MaxLimit+1)
		require.ErrorIs(t, err, feed.ErrInvalidLimit)
		_, err = client.RetrieveEvents(ctx, feed.After(-1), 20)
		require.ErrorIs(t, err, feed.ErrInvalidPosition)
		assert.Empty(t, recorded.requests)
	})

	t.Run("should report the status of a feed that fails", func(t *testing.T) {
		baseURL, _ := serveFeed(t, &fixture.EventStore{AfterErr: errors.New("an error occurred")}, "")
		client, err := feedhttp.NewClient(baseURL)
		require.NoError(t, err)

		batch, err := client.RetrieveEvents(ctx, feed.FromStart(), 20)
		var statusErr *feedhttp.StatusError
		require.ErrorAs(t, err, &statusErr)
		assert.Equal(t, http.StatusInternalServerError, statusErr.StatusCode)
		assert.Contains(t, err.Error(), "500 Internal Server Error")
		assert.Nil(t, batch)
	})

	t.Run("should report the status of a URL that serves no feed", func(t *testing.T) {
		baseURL, _ := serveFeed(t, &fixture.EventStore{}, "/orders")
		client, err := feedhttp.NewClient(baseURL + "/unknown")
		require.NoError(t, err)

		_, err = client.RetrieveEvents(ctx, feed.FromStart(), 20)
		var statusErr *feedhttp.StatusError
		require.ErrorAs(t, err, &statusErr)
		assert.Equal(t, http.StatusNotFound, statusErr.StatusCode)
	})

	t.Run("should fail on an answer that is not a batch", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("<html></html>"))
		}))
		t.Cleanup(server.Close)
		client, err := feedhttp.NewClient(server.URL)
		require.NoError(t, err)

		batch, err := client.RetrieveEvents(ctx, feed.FromStart(), 20)
		require.Error(t, err)
		assert.Nil(t, batch)
	})

	t.Run("should fail when the feed cannot be reached", func(t *testing.T) {
		server := httptest.NewServer(http.NotFoundHandler())
		server.Close()
		client, err := feedhttp.NewClient(server.URL)
		require.NoError(t, err)

		_, err = client.RetrieveEvents(ctx, feed.FromStart(), 20)
		require.Error(t, err)
	})

	t.Run("should fail when the context is done", func(t *testing.T) {
		baseURL, recorded := serveFeed(t, &fixture.EventStore{}, "")
		client, err := feedhttp.NewClient(baseURL)
		require.NoError(t, err)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		_, err = client.RetrieveEvents(cancelled, feed.FromStart(), 20)
		require.ErrorIs(t, err, context.Canceled)
		assert.Empty(t, recorded.requests)
	})
}

// TestConsumerOfARemoteFeed runs a consumer against the feed of another
// service, through HTTP.
func TestConsumerOfARemoteFeed(t *testing.T) {
	ctx := context.Background()
	sh := fixture.NewSomethingHappened()
	seh := fixture.NewSomethingElseHappened()

	// The publishing service.
	store := inmemory.NewEventStore(serializer.NewJSONSerializer(
		serializer.RegisterAs("SomethingHappened", &fixture.SomethingHappened{}),
		serializer.RegisterAs("SomethingElseHappened", &fixture.SomethingElseHappened{}),
	))
	for _, e := range []event.Event{sh, seh, sh, seh, sh} {
		require.NoError(t, store.Append(ctx, e))
	}
	baseURL, recorded := serveFeed(t, store, "/orders")

	// The consuming service, which is only interested in one type of event
	// and names it in its own language.
	client, err := feedhttp.NewClient(baseURL)
	require.NoError(t, err)
	positions := inmemory.NewPositionStore()
	consumer := feed.NewConsumer(
		client,
		serializer.NewJSONSerializer(serializer.RegisterAs("SomethingElseHappened", &somethingElseOccurred{})),
		positions,
		feed.WithBatchLimit(2),
	)
	var handled []*somethingElseOccurred
	consumer.Subscribe(event.HandledBy(func(_ context.Context, e *somethingElseOccurred) error {
		handled = append(handled, e)
		return nil
	}))

	require.NoError(t, consumer.CatchUp(ctx))
	require.Len(t, handled, 2)
	assert.Equal(t, seh.OccurredOn(), handled[0].OccurredOn())
	assert.Len(t, recorded.requests, 3, "5 events by batches of 2")
	position, found, err := positions.Position(ctx)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(5), position)

	require.NoError(t, store.Append(ctx, seh))
	require.NoError(t, consumer.CatchUp(ctx))
	assert.Len(t, handled, 3)
	assert.Equal(t, url.Values{"after": {"5"}, "limit": {"2"}}, recorded.last().URL.Query())
}

func TestConsumerOfARemoteFeedWithABinaryCodec(t *testing.T) {
	ctx := context.Background()
	recorded := fixture.NewSomethingRecorded("a reading")
	newSerializer := func() *serializer.Serializer {
		return serializer.NewProtoSerializer(serializer.RegisterAs("SomethingRecorded", &fixture.SomethingRecorded{}))
	}

	// The publishing service stores its events with the Protocol Buffers codec.
	store := inmemory.NewEventStore(newSerializer())
	require.NoError(t, store.Append(ctx, recorded))
	baseURL, _ := serveFeed(t, store, "")

	// The body stays valid JSON: the payload is a base64 string in the
	// envelope, which the feed embeds as it is.
	response, err := http.Get(baseURL + "/events")
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()
	var payload struct {
		Events []struct {
			Body struct {
				Type string `json:"type"`
				Data string `json:"data"`
			} `json:"body"`
		} `json:"events"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&payload))
	require.Len(t, payload.Events, 1)
	assert.Equal(t, "SomethingRecorded", payload.Events[0].Body.Type)
	data, err := base64.StdEncoding.DecodeString(payload.Events[0].Body.Data)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// The consuming service reads them with the same codec.
	client, err := feedhttp.NewClient(baseURL)
	require.NoError(t, err)
	consumer := feed.NewConsumer(client, newSerializer(), inmemory.NewPositionStore())
	checker := eventtest.NewChecker()
	consumer.Subscribe(checker)

	require.NoError(t, consumer.CatchUp(ctx))
	checker.AssertEvents(t, []event.Event{recorded})

	// A consumer with another codec must not skip the events: it fails.
	mismatched := feed.NewConsumer(
		client,
		serializer.NewJSONSerializer(serializer.RegisterAs("SomethingRecorded", &fixture.SomethingRecorded{})),
		inmemory.NewPositionStore(),
	)
	err = mismatched.CatchUp(ctx)
	require.Error(t, err)
	require.NotErrorIs(t, err, event.ErrTypeNotRegistered)
}

// somethingElseOccurred is the event SomethingElseHappened as the consuming
// service names it.
type somethingElseOccurred struct {
	When time.Time `json:"occurredOn"`
}

func (e *somethingElseOccurred) OccurredOn() time.Time {
	return e.When
}

// roundTripperFunc is a function used as a round tripper.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
