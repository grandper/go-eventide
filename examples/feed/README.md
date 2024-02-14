# Event feed: store, serve, consume, relay

This example follows the events of an *ordering* service until they reach a
*billing* service, the two ways the `feed` package offers.

## The idea

Publishing an event inside a process is not enough when another service needs
it. The ordering service therefore **stores** every event it publishes, and the
stored events form a **feed**: the events of the service, in the order they
were stored.

```
ordering service                                        billing service
----------------                                        ---------------
Publish(OrderPlaced)
   |
EventStoringSubscriber --> EventStore --> StoreFeed
                                            |-- pull: Controller -- HTTP --> Client --> Consumer --> subscribers
                                            `-- push: Relay --> message broker      (no HTTP involved)
```

| Step | What the example uses |
| ---- | --------------------- |
| Store the published events | `feed.NewEventStoringSubscriber(store)` registered in the scope |
| Turn the store into a feed | `feed.NewStoreFeed(store)` |
| Serve the feed over HTTP | `feedhttp.NewController(orders, store).Register(router)` |
| Pull: catch up from another service | `feedhttp.NewClient(url)` handed to `feed.NewConsumer(...)` |
| Push: publish on a message broker | `feed.NewRelay(orders, positions, broker)` |

Both the consumer and the relay remember their **position** in the feed (here in
an `inmemory.NewPositionStore()`), so they only deal with what happened since
the last time.

The two services do not share any Go type. The ordering service registers its
event under the name `OrderPlaced`, and the billing service reads it into its
own `OrderReceived` type, registered under the same name.

## Run it

```bash
go run ./examples/feed
```

Expected output:

```
[broker]  OrderPlaced {"version":1,"type":"OrderPlaced","data":{"OrderID":"41","At":"2026-09-26T10:00:00Z"}}
[broker]  OrderPlaced {"version":1,"type":"OrderPlaced","data":{"OrderID":"42","At":"2026-09-26T10:00:00Z"}}
[broker]  OrderPlaced {"version":1,"type":"OrderPlaced","data":{"OrderID":"43","At":"2026-09-26T10:00:00Z"}}
[billing] order 41 received
[billing] order 42 received
[billing] order 43 received
```

The second `CatchUp` of the example prints nothing: the consumer already went
past these events.

## Try this

- Replace `CatchUp` with `stop, err := consumer.Follow(ctx, time.Second)` and
  publish more events afterwards: they are handed on the next interval, until
  `stop` is called.
- Add `feed.StartingFrom(feed.FromEnd())` to the consumer: it ignores the
  orders placed before it came.
- Make `stdoutBroker.Publish` fail for one order and run the relay twice: the
  second run resumes at the order that failed.
