package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/grandper/go-serializer/serializer"

	"github.com/grandper/go-eventide/event"
	"github.com/grandper/go-eventide/feed"
	"github.com/grandper/go-eventide/feed/feedhttp"
	"github.com/grandper/go-eventide/feed/inmemory"
)

// OrderPlaced is a domain event of the ordering service.
type OrderPlaced struct {
	// OrderID identifies the order that was placed.
	OrderID string
	// At is when the order was placed.
	At time.Time
}

// OccurredOn reports when the event happened, satisfying event.Event.
func (e OrderPlaced) OccurredOn() time.Time { return e.At }

// OrderReceived is the same event, as the billing service names it. It only
// holds the fields billing needs.
type OrderReceived struct {
	// OrderID identifies the order billing has to invoice.
	OrderID string
	// At is when the order was placed.
	At time.Time
}

// OccurredOn reports when the event happened, satisfying event.Event.
func (e OrderReceived) OccurredOn() time.Time { return e.At }

// stdoutBroker stands for a message broker: it prints the messages it is
// asked to publish.
type stdoutBroker struct{}

// Publish prints the message, satisfying event.MessagePublisher.
func (stdoutBroker) Publish(_ context.Context, key string, message []byte) error {
	fmt.Printf("[broker]  %s %s\n", key, message)
	return nil
}

func main() {
	ctx := context.Background()

	// --- The ordering service -------------------------------------------

	// Every event published in the scope is stored. The events are
	// registered under a name: it is what the other services read.
	store := inmemory.NewEventStore(serializer.NewJSONSerializer(serializer.RegisterAs("OrderPlaced", OrderPlaced{})))
	event.PrivateScope(func(ctx context.Context) {
		for _, orderID := range []string{"41", "42", "43"} {
			placed := OrderPlaced{OrderID: orderID, At: time.Date(2026, time.September, 26, 10, 0, 0, 0, time.UTC)}
			if err := event.Publish(ctx, placed); err != nil {
				log.Fatalf("publish OrderPlaced: %v", err)
			}
		}
	}, feed.NewEventStoringSubscriber(store))

	// The stored events are served as a feed, over HTTP. The controller is
	// given the store too: it deletes events on DELETE requests.
	orders := feed.NewStoreFeed(store)
	router := mux.NewRouter()
	feedhttp.NewController(orders, store).Register(router)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	go func() { _ = http.Serve(listener, router) }()

	// Push: the relay publishes the events of the feed on a message broker.
	relay := feed.NewRelay(orders, inmemory.NewPositionStore(), stdoutBroker{})
	if err := relay.PublishPendingEvents(ctx); err != nil {
		log.Fatalf("relay: %v", err)
	}

	// --- The billing service --------------------------------------------

	// Pull: the consumer catches up with the feed of the ordering service.
	remoteOrders, err := feedhttp.NewClient("http://" + listener.Addr().String())
	if err != nil {
		log.Fatalf("feed client: %v", err)
	}
	consumer := feed.NewConsumer(
		remoteOrders,
		serializer.NewJSONSerializer(serializer.RegisterAs("OrderPlaced", OrderReceived{})),
		inmemory.NewPositionStore(),
	)
	consumer.Subscribe(event.HandledBy(func(_ context.Context, e OrderReceived) error {
		fmt.Printf("[billing] order %s received\n", e.OrderID)
		return nil
	}))
	if err := consumer.CatchUp(ctx); err != nil {
		log.Fatalf("catch up: %v", err)
	}

	// Catching up again hands nothing: the consumer remembers its position.
	if err := consumer.CatchUp(ctx); err != nil {
		log.Fatalf("catch up: %v", err)
	}
}
