# HTTP middleware integration

This example wires the `event` package into an HTTP server built with
[`gorilla/mux`](https://github.com/gorilla/mux).

## The idea

`middleware.PublisherWithSubscribers(subscribers...)` returns a mux middleware
that, for **every incoming request**, opens a fresh event scope (via
`event.PrivateScopeWithContext`) seeded with the given subscribers. The scope
lives in the request's `context.Context`.

Handlers stay oblivious to observers: they call
`event.Publish(r.Context(), someEvent)` and the middleware decides what happens
to the event. Here a `LoggingSubscriber` logs every published event.

```
request --> [middleware: open scope + LoggingSubscriber] --> handler: Publish(SomethingHappened)
                                                                          |
                                                                LoggingSubscriber logs it
```

Because each request gets its own scope, subscribers never leak state between
requests.

## Run it

```bash
go run ./examples/middleware
```

Then, in another terminal:

```bash
curl http://localhost:8080/
# -> event published
```

The server logs the event published during the request, after the line it logged
when it started (the date and time are the ones of the run):

```
2026/09/26 10:00:00 listening on http://localhost:8080 (GET / to publish an event)
2026/09/26 10:00:05 INFO event published event=SomethingHappened
```

## Try this

- Add your own subscriber to the `PublisherWithSubscribers(...)` call to fan the
  event out to more than one observer.
- Use `middleware.Publisher` (no subscribers) when handlers register their own
  subscribers per request with `event.Subscribe(r.Context(), ...)`.
