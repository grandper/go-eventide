# gRPC interceptor integration

This example wires the `event` package into a [gRPC](https://grpc.io) server. It
is the gRPC counterpart of the [`middleware`](../middleware) example.

## The idea

gRPC interceptors are to RPCs what HTTP middleware is to requests. The
`interceptor` package provides interceptors that open a fresh event scope for
every call, so service methods can publish events with
`event.Publish(ctx, ...)` without ever seeing a publisher.

| Interceptor                                          | Applies to     | Scope contents                              |
| ---------------------------------------------------- | -------------- | ------------------------------------------- |
| `interceptor.Publisher`                              | unary RPCs     | empty scope; the handler adds subscribers   |
| `interceptor.PublisherWithSubscribers(subs...)`      | unary RPCs     | scope seeded with `subs`                    |
| `interceptor.StreamPublisher`                        | streaming RPCs | empty scope                                 |
| `interceptor.StreamPublisherWithSubscribers(subs...)`| streaming RPCs | scope seeded with `subs`                    |

For streaming RPCs the interceptor wraps the `grpc.ServerStream` so that
`stream.Context()` carries the scoped publisher.

## A template, not a full server

This file is intentionally minimal: it installs the interceptors but registers
no service, so `Serve` just blocks. To make it do something, generate a service
from a `.proto`, register it, and publish events from its methods:

```go
// Register your generated service here
pb.RegisterGreeterServer(server, &greeterServer{})

// ...and inside a method:
func (s *greeterServer) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
    _ = event.Publish(ctx, NewGreetedEvent(in.GetName())) // observed by the interceptor's subscribers
    return &pb.HelloReply{Message: "hello " + in.GetName()}, nil
}
```

## Run it

```bash
go run ./examples/interceptor
# -> 2026/09/26 10:00:00 gRPC server listening on localhost:8080
```

The date and time are the ones of the run.

Press `Ctrl+C` to stop it.
