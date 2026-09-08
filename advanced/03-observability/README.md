# advanced/03 — metrics, health and traces

The three questions somebody asks about a running service, and where each
answer comes from: how much is moving, whether it is working, and which message
it was.

## What it shows

- **Metrics**, with names shared across all five AceMQ libraries, rendered as
  Prometheus text.
- **A health report** that proves a round trip to the broker rather than an
  open socket — and the difference between `up` and `degraded`.
- **Spans** that join across the broker, read out of the message's own headers.

## Running it

```bash
docker compose up -d --wait
bundle exec ruby advanced/03-observability/main.rb
```

## What to look for

```
health while consuming: up
  consumers: 1
  consumers_running: 1
  queues: ["warehouse.picks"]
  round_trip_ms: 21

health after cancelling the consumer: degraded
  1 of 1 consumers has stopped

metrics:
  acemq_messages_consumed{queue="warehouse.picks"} 3
  acemq_messages_accepted{queue="warehouse.picks"} 2
  acemq_messages_rejected{queue="warehouse.picks"} 1
  acemq_messages_dead_lettered{queue="warehouse.picks"} 1
  acemq_messages_published{exchange="warehouse-events"} 3
  ...

spans:
  warehouse-events publish     producer confirmed
  warehouse.picks process      consumer acked
  warehouse.picks process      consumer rejected
```

## Health: `degraded` is not `down`

**A consumer that has stopped under a live connection is `degraded`.** The
process can still publish and its other consumers still work, so failing the
readiness probe would take out something doing most of its job — but a queue
with nothing reading it is a real fault and has to be visible. That is what the
third status is for, and running the check twice here is the shortest way to
show the difference.

The broker is checked by **declaring a queue and deleting it again**, because
that is the cheapest thing AMQP offers that actually proves the round trip: an
open socket answers the same as a healthy broker right up until something is
asked of it. It costs a round trip, so it belongs on a probe's interval rather
than in a request.

`Health.aggregate` combines this with the application's own checks — anything
answering `name` and `check` — runs them on threads, and takes the worst
answer.

## Metrics: no dependency on a metrics gem

An observer is anything answering `count`, `observe` and `gauge`. Depending on
a metrics library would put every service using this one on the same choice,
and that choice belongs to the application. `Telemetry::Registry` is a working
in-memory implementation for when the numbers themselves are what is wanted.

`to_prometheus` returns a **string**, not a Rack app. This library has no web
framework and should not choose one; serve it from whatever already answers
HTTP in your process — and serve it on a port the ingress does not publish,
because what a service publishes and how long its handlers take is more than an
anonymous caller should be able to learn.

Anything an observer raises is swallowed and reported once per metric on
stderr. A metrics backend that is down is not a reason to stop delivering
messages.

**`acemq.retry.rung.missing` is worth an alert**, though nothing here produces
it. A retry long enough to be handed to the broker checks that its rung queue
is really there first, because a publish into a queue nobody declared is
dropped without a word. When the rung is missing the wait happens in the
consumer instead, so nothing is lost — what is lost is the reason the rung
exists, since a restart mid-wait now turns a five-minute backoff into none.

## Traces: the join is the point

The consumer span's parent comes out of **the message's own headers**, not out
of ambient context. That join, across processes and minutes, is the entire
reason to trace a message system — and the example checks it, by comparing the
trace identifiers of the publish spans and the process spans.

The trace travels in `traceparent` and `tracestate`, the W3C names,
deliberately **not** `x-acemq-` prefixed: other tooling already knows them, and
the Java library writes the same two, so a Ruby consumer joins a Java
producer's trace with neither side configured for the other.

`unroutable`, `failed` and `dead_lettered` make a span an error; `acked`,
`retried` and `rejected` do not — a message that will be tried again has not
failed yet, and a trace view where every retry is red stops meaning anything.
Retries, dead letters, outbox failures and finished pipeline runs are *events*
on the span already open, because a zero-length span at the end of a trace adds
a row and no information.

The adapter registers at `order` `-1000`, so its span covers whatever the other
interceptors do rather than sitting inside them.

## The SDK is here only to show the spans

The adapter needs `opentelemetry-api` and nothing else. `opentelemetry-sdk` is
in this repository's Gemfile because an in-memory exporter is the only way to
print the spans that were actually recorded without standing up a collector. In
a service the exporter is OTLP and the collector is somewhere else.

Both are pinned below their current versions on Ruby 3.1 and 3.2 — see the
comment in the Gemfile — because they raised their floors past the version this
library still promises.
