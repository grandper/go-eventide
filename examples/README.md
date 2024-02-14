# Examples

Runnable examples for the [`Eventide`](../README.md) library. Each directory has
its own README with a walkthrough.

| Example                        | What it shows                                                        | Dependencies         |
| ------------------------------ | ------------------------------------------------------------------- | -------------------- |
| [`pubsub`](./pubsub)           | Scopes and context-based publish/subscribe; event propagation.      | none                 |
| [`middleware`](./middleware)   | Opening an event scope per HTTP request with `gorilla/mux`.         | none                 |
| [`interceptor`](./interceptor) | Opening an event scope per gRPC call with server interceptors.      | none                 |
| [`feed`](./feed)               | Storing events, serving them as a feed, consuming and relaying it.  | none                 |

They all run with no setup:

```bash
go run ./examples/pubsub
go run ./examples/middleware
go run ./examples/interceptor
go run ./examples/feed
```

Start with [`pubsub`](./pubsub): the scope mechanics it demonstrates underpin the
middleware and interceptor examples. [`feed`](./feed) shows how the events
then reach other services.
