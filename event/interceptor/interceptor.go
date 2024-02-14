package interceptor

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grandper/go-eventide/event"
)

// Publisher is a unary server interceptor that opens a private event scope for
// the duration of the call, so the handler can publish through its context.
func Publisher(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	var resp any
	var err error
	event.PrivateScopeWithContext(ctx, func(ctx context.Context) {
		resp, err = handler(ctx, req)
	})
	return resp, err
}

// PublisherWithSubscribers returns a unary server interceptor that opens a
// private event scope for the duration of the call, with the provided
// subscribers already registered.
func PublisherWithSubscribers(subscribers ...event.Subscriber) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		var resp any
		var err error
		event.PrivateScopeWithContext(ctx, func(ctx context.Context) {
			resp, err = handler(ctx, req)
		}, subscribers...)
		return resp, err
	}
}

// StreamPublisher is a stream server interceptor that opens a private event
// scope for the duration of the stream, so the handler can publish through the
// stream's context.
func StreamPublisher(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	var err error
	event.PrivateScopeWithContext(stream.Context(), func(ctx context.Context) {
		newStream := &serverStreamWrapper{
			ss:  stream,
			ctx: ctx,
		}
		err = handler(srv, newStream)
	})
	return err
}

// StreamPublisherWithSubscribers returns a stream server interceptor that
// opens a private event scope for the duration of the stream, with the
// provided subscribers already registered.
func StreamPublisherWithSubscribers(subscribers ...event.Subscriber) grpc.StreamServerInterceptor {
	return func(
		srv any,
		stream grpc.ServerStream,
		_ *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		var err error
		event.PrivateScopeWithContext(stream.Context(), func(ctx context.Context) {
			newStream := &serverStreamWrapper{
				ss:  stream,
				ctx: ctx,
			}
			err = handler(srv, newStream)
		}, subscribers...)
		return err
	}
}

type serverStreamWrapper struct {
	ss  grpc.ServerStream
	ctx context.Context
}

func (w serverStreamWrapper) Context() context.Context        { return w.ctx }
func (w serverStreamWrapper) RecvMsg(msg interface{}) error   { return w.ss.RecvMsg(msg) }
func (w serverStreamWrapper) SendMsg(msg interface{}) error   { return w.ss.SendMsg(msg) }
func (w serverStreamWrapper) SendHeader(md metadata.MD) error { return w.ss.SendHeader(md) }
func (w serverStreamWrapper) SetHeader(md metadata.MD) error  { return w.ss.SetHeader(md) }
func (w serverStreamWrapper) SetTrailer(md metadata.MD)       { w.ss.SetTrailer(md) }
