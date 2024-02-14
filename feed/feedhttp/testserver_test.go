package feedhttp_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

// Registerer is an interface for a service that needs to be registered in a router.
type Registerer interface {
	Register(r *mux.Router)
}

// TestHTTPServer is a server to test a service.
type TestHTTPServer struct {
	router *mux.Router
}

// NewTestHTTPServer creates a test server.
func NewTestHTTPServer(registerer Registerer) *TestHTTPServer {
	r := mux.NewRouter()
	registerer.Register(r)
	return &TestHTTPServer{
		router: r,
	}
}

// NewTestHTTPServerUnder creates a test server whose service is registered
// on a sub-router, under the path prefix.
func NewTestHTTPServerUnder(pathPrefix string, registerer Registerer) *TestHTTPServer {
	r := mux.NewRouter()
	registerer.Register(r.PathPrefix(pathPrefix).Subrouter())
	return &TestHTTPServer{
		router: r,
	}
}

// ExecuteRequest executes the request on the server.
func (ths *TestHTTPServer) ExecuteRequest(req *http.Request) *TestResponse {
	rr := httptest.NewRecorder()
	ths.router.ServeHTTP(rr, req)
	return &TestResponse{
		response: rr,
	}
}

// TestResponse is a test object that can be used to assert the response attributes.
type TestResponse struct {
	response *httptest.ResponseRecorder
}

// AssertStatus asserts the value of the http status code.
func (tr *TestResponse) AssertStatus(t *testing.T, expected int) bool {
	t.Helper()
	return assert.Equal(
		t,
		expected,
		tr.response.Code,
		"expected response code %d. Got %d\n",
		expected,
		tr.response.Code,
	)
}

// AssertHeader asserts the value of a header of the response.
func (tr *TestResponse) AssertHeader(t *testing.T, name, expected string) bool {
	t.Helper()
	return assert.Equal(t, expected, tr.response.Header().Get(name), "unexpected value of the header %s", name)
}

// AssertBodyString asserts the value of the response body as a string.
func (tr *TestResponse) AssertBodyString(t *testing.T, expected string) bool {
	t.Helper()
	bodyStr := tr.response.Body.String()
	return assert.Equal(t, expected, bodyStr, "expected response body %d. Got %d\n", expected, bodyStr)
}

// AssertContentType asserts the content type of the response.
func (tr *TestResponse) AssertContentType(t *testing.T, expected string) bool {
	t.Helper()
	contentType := tr.response.Header().Get("Content-Type")
	return assert.Equal(t, expected, contentType, "expected response content type %d. Got %d\n", expected, contentType)
}
