package interceptor //nolint:testpackage // white-box test: constructs the unexported serverStreamWrapper

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/internal/fixture"
)

type testSubscriber struct {
	eventOccurred bool
}

func (ts *testSubscriber) Handle(context.Context, event.Event) error {
	ts.eventOccurred = true
	return nil
}

func TestInterceptor(t *testing.T) {
	ctx := context.Background()
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: "FakeMethod",
	}

	t.Run("should trigger an event when an endpoint is triggered", func(t *testing.T) {
		s := &testSubscriber{}
		handlerWithTrigger := func(ctx context.Context, _ any) (any, error) {
			event.Subscribe(ctx, s)
			event.Publish(ctx, fixture.NewSomethingHappened())
			return struct{}{}, nil
		}
		resp, err := Publisher(ctx, nil, serverInfo, handlerWithTrigger)
		assert.NotNil(t, resp)
		require.NoError(t, err)
		assert.True(t, s.eventOccurred)
	})

	t.Run("should not trigger an event when an endpoint does not trigger", func(t *testing.T) {
		s := &testSubscriber{}
		handlerWithoutTrigger := func(ctx context.Context, _ any) (any, error) {
			event.Subscribe(ctx, s)
			return struct{}{}, nil
		}
		resp, err := Publisher(ctx, nil, serverInfo, handlerWithoutTrigger)
		assert.NotNil(t, resp)
		require.NoError(t, err)
		assert.False(t, s.eventOccurred)
	})

	t.Run("should trigger an event when an endpoint is triggered", func(t *testing.T) {
		s := &testSubscriber{}
		unaryInterceptor := PublisherWithSubscribers(s)
		handlerWithTrigger := func(ctx context.Context, _ any) (any, error) {
			event.Publish(ctx, fixture.NewSomethingHappened())
			return struct{}{}, nil
		}
		resp, err := unaryInterceptor(ctx, nil, serverInfo, handlerWithTrigger)
		assert.NotNil(t, resp)
		require.NoError(t, err)
		assert.True(t, s.eventOccurred)
	})

	t.Run("should not trigger an event when an endpoint does not trigger", func(t *testing.T) {
		s := &testSubscriber{}
		unaryInterceptor := PublisherWithSubscribers(s)
		handlerWithoutTrigger := func(_ context.Context, _ any) (any, error) {
			return struct{}{}, nil
		}
		resp, err := unaryInterceptor(ctx, nil, serverInfo, handlerWithoutTrigger)
		assert.NotNil(t, resp)
		require.NoError(t, err)
		assert.False(t, s.eventOccurred)
	})

	t.Run("should simply return an error if the handler return an error", func(t *testing.T) {
		s := &testSubscriber{}
		handlerWithErr := func(ctx context.Context, _ any) (any, error) {
			event.Subscribe(ctx, s)
			return nil, errors.New("an error occurred")
		}
		resp, err := Publisher(ctx, nil, serverInfo, handlerWithErr)
		assert.Nil(t, resp)
		require.Error(t, err)
		assert.False(t, s.eventOccurred)
	})

	srv := struct{}{}
	stream := serverStreamWrapper{
		ss:  nil,
		ctx: ctx,
	}
	streamServerInfo := &grpc.StreamServerInfo{
		FullMethod: "FakeMethod",
	}

	t.Run("should trigger an event when an endpoint is triggered in the stream", func(t *testing.T) {
		s := &testSubscriber{}
		handlerWithTrigger := func(_ any, stream grpc.ServerStream) error {
			event.Subscribe(stream.Context(), s)
			event.Publish(stream.Context(), fixture.NewSomethingHappened())
			return nil
		}
		err := StreamPublisher(srv, stream, streamServerInfo, handlerWithTrigger)
		require.NoError(t, err)
		assert.True(t, s.eventOccurred)
	})

	t.Run("should not trigger an event when an endpoint does not trigger in the stream", func(t *testing.T) {
		s := &testSubscriber{}
		handlerWithoutTrigger := func(_ any, stream grpc.ServerStream) error {
			event.Subscribe(stream.Context(), s)
			return nil
		}
		err := StreamPublisher(srv, stream, streamServerInfo, handlerWithoutTrigger)
		require.NoError(t, err)
		assert.False(t, s.eventOccurred)
	})

	t.Run("should trigger an event when an endpoint is triggered in the stream", func(t *testing.T) {
		s := &testSubscriber{}
		streamInterceptor := StreamPublisherWithSubscribers(s)
		handlerWithTrigger := func(_ any, stream grpc.ServerStream) error {
			event.Publish(stream.Context(), fixture.NewSomethingHappened())
			return nil
		}
		err := streamInterceptor(srv, stream, streamServerInfo, handlerWithTrigger)
		require.NoError(t, err)
		assert.True(t, s.eventOccurred)
	})

	t.Run("should not trigger an event when an endpoint does not trigger in the stream", func(t *testing.T) {
		s := &testSubscriber{}
		streamInterceptor := StreamPublisherWithSubscribers(s)
		handlerWithoutTrigger := func(_ any, _ grpc.ServerStream) error {
			return nil
		}
		err := streamInterceptor(srv, stream, streamServerInfo, handlerWithoutTrigger)
		require.NoError(t, err)
		assert.False(t, s.eventOccurred)
	})

	t.Run("should simply return an error if the handler return an error in a stream", func(t *testing.T) {
		s := &testSubscriber{}
		handlerWithErr := func(_ any, stream grpc.ServerStream) error {
			event.Subscribe(stream.Context(), s)
			return errors.New("an error occurred")
		}
		err := StreamPublisher(srv, stream, streamServerInfo, handlerWithErr)
		require.Error(t, err)
		assert.False(t, s.eventOccurred)
	})

	t.Run("should pass the stream calls through to the wrapped stream", func(t *testing.T) {
		wrapped := &testServerStream{
			ctx: ctx,
			err: errors.New("an error occurred"),
		}
		header := metadata.Pairs("key", "header")
		trailer := metadata.Pairs("key", "trailer")
		handler := func(_ any, stream grpc.ServerStream) error {
			require.ErrorIs(t, stream.RecvMsg("received"), wrapped.err)
			require.ErrorIs(t, stream.SendMsg("sent"), wrapped.err)
			require.ErrorIs(t, stream.SendHeader(header), wrapped.err)
			require.ErrorIs(t, stream.SetHeader(header), wrapped.err)
			stream.SetTrailer(trailer)
			return nil
		}
		err := StreamPublisher(srv, wrapped, streamServerInfo, handler)
		require.NoError(t, err)
		assert.Equal(t, "received", wrapped.received)
		assert.Equal(t, "sent", wrapped.sent)
		assert.Equal(t, header, wrapped.sentHeader)
		assert.Equal(t, header, wrapped.header)
		assert.Equal(t, trailer, wrapped.trailer)
	})
}

// testServerStream is a server stream that records the calls it receives, and
// answers them with its error.
type testServerStream struct {
	ctx context.Context
	err error

	received   any
	sent       any
	sentHeader metadata.MD
	header     metadata.MD
	trailer    metadata.MD
}

func (s *testServerStream) Context() context.Context { return s.ctx }

func (s *testServerStream) RecvMsg(msg any) error {
	s.received = msg
	return s.err
}

func (s *testServerStream) SendMsg(msg any) error {
	s.sent = msg
	return s.err
}

func (s *testServerStream) SendHeader(md metadata.MD) error {
	s.sentHeader = md
	return s.err
}

func (s *testServerStream) SetHeader(md metadata.MD) error {
	s.header = md
	return s.err
}

func (s *testServerStream) SetTrailer(md metadata.MD) { s.trailer = md }
