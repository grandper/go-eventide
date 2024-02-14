package feedhttp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/mux"

	"github.com/grandper/go-eventide/feed"
)

// Client reads the feed another service serves with a Controller. It is a
// feed.Feed, to hand to a feed.Consumer.
type Client struct {
	httpClient *http.Client
	eventsURL  *url.URL
}

// ClientOption is an option of NewClient.
type ClientOption func(*Client)

// WithHTTPClient sets the HTTP client of the calls (timeout, authentication,
// tracing). The default is http.DefaultClient.
func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// NewClient creates a client of the feed served under the given base URL, the
// URL the router of the Controller is mounted on.
func NewClient(baseURL string, options ...ClientOption) (*Client, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create the feed client: %w", err)
	}
	if base.Scheme == "" || base.Host == "" {
		return nil, fmt.Errorf("failed to create the feed client: the base URL '%s' is not absolute", baseURL)
	}
	r := mux.NewRouter()
	routes(r)
	eventsPath, err := r.Get(EventsRoute).URLPath()
	if err != nil {
		return nil, fmt.Errorf("failed to create the feed client: %w", err)
	}
	c := &Client{
		httpClient: http.DefaultClient,
		eventsURL:  base.JoinPath(eventsPath.Path),
	}
	for _, option := range options {
		option(c)
	}
	return c, nil
}

// StatusError is returned when the feed answers with another status than 200.
type StatusError struct {
	// StatusCode is the HTTP status code the feed answered with.
	StatusCode int
}

// Error returns the message of the error.
func (e *StatusError) Error() string {
	return fmt.Sprintf("the feed answered with the status %d %s", e.StatusCode, http.StatusText(e.StatusCode))
}

// RetrieveEvents returns at most limit events of the remote feed from the origin. A
// limit of 0 stands for feed.DefaultLimit. It fails with a *StatusError when
// the feed answers with another status than 200.
func (c *Client) RetrieveEvents(ctx context.Context, origin feed.Origin, limit int) (*feed.Batch, error) {
	requestURL, err := c.requestURL(origin, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to read the feed: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read the feed: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("failed to read the feed: %w", err)
	}
	// The answer is read before the body is closed: a failure to close it
	// does not change what was read.
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to read the feed: %w", &StatusError{StatusCode: response.StatusCode})
	}
	var payload batchResponse
	if decodeErr := json.NewDecoder(response.Body).Decode(&payload); decodeErr != nil {
		return nil, fmt.Errorf("failed to read the feed: %w", decodeErr)
	}
	return payload.batch(), nil
}

// requestURL returns the URL of the events of the feed from the origin.
func (c *Client) requestURL(origin feed.Origin, limit int) (string, error) {
	limit, err := feed.NormalizeLimit(limit)
	if err != nil {
		return "", err
	}
	query := url.Values{}
	switch origin.Kind() {
	case feed.OriginStart:
		query.Set(FromParam, FromStart)
	case feed.OriginEnd:
		query.Set(FromParam, FromEnd)
	case feed.OriginTime:
		query.Set(SinceParam, origin.Time().UTC().Format(time.RFC3339Nano))
	case feed.OriginPosition:
		if origin.Position() < 0 {
			return "", feed.ErrInvalidPosition
		}
		query.Set(AfterParam, strconv.FormatInt(origin.Position(), 10))
	default:
		return "", fmt.Errorf("unknown kind of origin: %d", origin.Kind())
	}
	query.Set(LimitParam, strconv.Itoa(limit))
	requestURL := *c.eventsURL
	requestURL.RawQuery = query.Encode()
	return requestURL.String(), nil
}

// batch turns the payload into a batch.
func (r batchResponse) batch() *feed.Batch {
	events := make([]*feed.StoredEvent, 0, len(r.Events))
	for _, e := range r.Events {
		events = append(events, feed.NewStoredEvent(e.ID, e.Type, e.OccurredOn, bodyFromJSON(e.Body)))
	}
	return feed.NewBatch(events, r.Next, r.HasMore)
}

// bodyFromJSON is the reverse of bodyToJSON: a JSON string is a body that was
// carried in base64, and any other JSON value is the body as it is.
func bodyFromJSON(body json.RawMessage) []byte {
	var decoded []byte
	if err := json.Unmarshal(body, &decoded); err == nil {
		return decoded
	}
	return body
}

// StatusError implements the error interface.
var _ error = &StatusError{}

// Client implements the feed.Feed interface.
var _ feed.Feed = &Client{}
