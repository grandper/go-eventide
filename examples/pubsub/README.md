# Scopes & context-based pub/sub

This example shows the core of the `event` package: publishing and subscribing
through the `context.Context`, and how events propagate up through nested
**scopes**.

## The idea

Instead of threading a `*event.Publisher` through every function signature, you
store it in the context. A **scope** owns a publisher for the duration of a
function call. Subscribers registered in a scope only live as long as that call.

A request here flows through three layers, each a scope nested in the previous
one:

```
serverCall           opens the outer scope  -> "server" subscriber
  applicationCall      opens a sub-scope     -> "application" subscriber
    domainCall           publishes HelloEvent
```

`domainCall` only calls `event.Publish(ctx, ...)`. It does not know who is
listening. The event reaches the sub-scope's subscriber (`application`) and is
then **propagated up** to the parent scope's subscriber (`server`).

## Scope helpers used

| Call                                    | Behaviour                                                              |
| --------------------------------------- | --------------------------------------------------------------------- |
| `event.PrivateScopeWithContext(ctx, fn, subs...)` | Fresh, self-contained publisher. Events published inside stay inside. |
| `event.Scope(ctx, fn, subs...)`         | Sub-scope: its subscribers see its events, which are **also** forwarded to the parent scope. |

That difference is the whole example: `serverCall` uses a private scope to
establish the outer publisher, and `applicationCall` uses `event.Scope` so the
domain event bubbles back up to the server.

## Run it

```bash
go run ./examples/pubsub
```

Expected output (both layers receive the single published event; the date and
time are the ones of the run):

```
2026/09/26 10:00:00 [application] received HelloEvent
2026/09/26 10:00:00 [server] received HelloEvent
```

## Try this

- Swap `event.Scope` for `event.PrivateScopeWithContext` in `applicationCall`
  and re-run: the event no longer reaches the `server` subscriber, because a
  private scope does not propagate upward.
- Publish a second event type from `domainCall` and register a typed handler
  with `event.Handle(ctx, func(context.Context, *HelloEvent) error { ... })` to
  react to just one kind of event.
