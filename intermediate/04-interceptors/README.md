# intermediate/04 — interceptors

A tenant stamped on every message and every handler timed, with neither
appearing in the handler — and a message that should not go out stopped before
it does.

## What it shows

- **A block is the common case**, an object is the full one. The object here
  answers `before_handle`, `after_handle` and `order`; it could also answer
  `on_error`.
- **Raising means different things in different places, on purpose.**
- **`set_header` refuses the reserved `x-acemq-` names.**

## Running it

```bash
docker compose up -d --wait
bundle exec ruby intermediate/04-interceptors/main.rb
```

## What to look for

```
handled:  T-1
refused:  refusing to publish a ticket with no subject
timed:    tenant=acme outcome=accept 12.8ms
reserved: these header names belong to AceMQ and cannot be set by hand: x-acemq-attempt
```

The handler block in `main.rb` mentions neither the tenant nor the clock. That
is the claim: these are things every message in an organisation needs and no
library can guess — a tenant, a trace context, a log scope, a size limit, a
metric — and without a seam for them they end up copied into every call site,
where one of them is always the one that forgot.

The `timed:` line reads the tenant off the envelope on the **consuming** side,
which is how you can tell the header really travelled.

## Where raising takes you

- **From `before_publish` it stops the publish** and the caller sees the
  exception. That is the point of intercepting rather than observing: a message
  that must not go out is stopped once, here, rather than in every publisher.
- **From `before_handle` the handler never runs** and the delivery is treated
  exactly as a failed handler would be — retried, then dead-lettered. An
  interceptor that refuses a message has to be willing for that message to
  reach the dead-letter queue, which is the honest outcome; the alternative is
  acknowledging something nothing processed. `FatalError` still means what it
  means.
- **From `after_confirm`, `after_handle` or `on_error` it is reported on stderr
  and otherwise ignored.** The message has been sent or the delivery settled,
  and letting the exception out would report a successful publish as a failed
  one.

## Order

Lower `order` runs first, equal orders run in registration order, and the way
out of a handler is **reversed** — so a pair that opens something on the way in
and closes it on the way out nests properly. The timing interceptor here uses
`-100` so it wraps whatever else is registered.

The OpenTelemetry adapter in [advanced/03](../../advanced/03-observability) uses
`-1000` for the same reason: its span should cover what the other interceptors
do, not sit inside them.

## Why the reserved names are refused

Silently dropping a header somebody deliberately set is worse than saying no —
and letting one through would let an interceptor rewrite the attempt counter
the retry engine is counting on. What an interceptor leaves on the envelope is
what the handler receives **and** what any dead letter is written with; a
header the handler saw and the dead-letter queue did not would be missing
exactly when somebody goes looking for it.

## Threads

An interceptor is called on whatever thread is publishing or handling, so one
that keeps state has to be safe to call from several at once. The `Timing`
class here uses a mutex for that reason and not for any other.
