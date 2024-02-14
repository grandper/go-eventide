package main

import (
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/event/interceptor"
)

const address = "localhost:8080"

func main() {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", address, err)
	}

	server := grpc.NewServer(
		// Unary RPCs: open a scope per call. Handlers register their own
		// subscribers with event.Subscribe(ctx, ...).
		grpc.UnaryInterceptor(interceptor.Publisher),
		// Streaming RPCs: open a scope per stream, with a LoggingSubscriber
		// already attached so every published event is logged.
		grpc.StreamInterceptor(interceptor.StreamPublisherWithSubscribers(event.NewLoggingSubscriber())),
	)

	// Register your generated service here, e.g.:
	//   pb.RegisterGreeterServer(server, &greeterServer{})

	log.Printf("gRPC server listening on %s", address)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("server stopped: %v", err)
	}
}
