# advanced/05 — a standing load something else can watch

A load that does not finish. It publishes and consumes at a steady rate and prints one
JSON object per second saying what has happened to it, so that something outside the
process can read what the client saw rather than what the broker did.

Every other example here runs, proves a point and exits. This one is the thing a fault
drill breaks the cluster underneath.

## What it shows

- **A client reporting its own state**, in a shape another program can read: one JSON
  object per line on stdout, oldest first.
- **`blocked?` and `blocked_reason` on the same line**, so a reading cannot claim to be
  blocked without saying why.
- **Publishes bounded by `Timeout.timeout`.** A publish on a blocked connection does not
  raise — it waits, because RabbitMQ has stopped reading the socket. Without a deadline
  the sampler would wait with it, and a client that went quiet looks exactly like a
  client that was never running.
- **Counters behind a mutex**, because the consumer runs on bunny's own thread and the
  sampler on the main one. The GVL makes a torn read unlikely rather than impossible,
  and "unlikely" is not a property to leave in a program whose entire output is numbers
  somebody will reason about.
- **Readings flushed as they are taken**, because a drill reads this output while the
  process is still running.

## Running it

```bash
docker compose up -d

bundle exec ruby advanced/05-a-standing-load-something-else-can-watch/main.rb \
    > readings.jsonl
```

Environment: `ACEMQ_URL`, `ACEMQ_LOAD_RATE` (per second, default 200),
`ACEMQ_LOAD_INTERVAL` (seconds between readings), `ACEMQ_EXAMPLE_SECONDS` (stop after
this long; unset runs until interrupted, which is what a drill campaign wants).

## What it prints

```json
{"at":"2026-09-26T19:29:44Z","elapsedMs":4009,"blocked":false,"published":453,"confirmed":452,"consumed":452,"failed":0,"publishRate":59.69,"consumeRate":59.69}
```

Under a memory alarm the same line reads:

```json
{"at":"...","elapsedMs":585642,"blocked":true,"published":59337,"confirmed":59315,"consumed":59315,"failed":21,"reason":"low on memory","publishRate":0.0,"consumeRate":0.0}
```

`confirmed` has stopped moving while `published` keeps climbing, `failed` counts the
sends that never completed, and `reason` is the broker's own words. None of that is
visible to anything that asks the broker how it is doing — the cluster is healthy, and
this application is not publishing.

## The fields

| Field | Is |
|---|---|
| `blocked` | the broker is refusing to read from this connection now |
| `published` | sends attempted since the start |
| `confirmed` | sends the broker has acknowledged |
| `consumed` | deliveries handled |
| `failed` | sends that did not complete |
| `publishRate` / `consumeRate` | confirms and deliveries per second over the last interval |
| `reason` | what the broker said when it blocked the connection |

The first seven names are a contract with whatever reads them, so they are not renamed
for tidiness: a reader looking for `confirmed` and finding `acked` sees a client
reporting nothing, which is indistinguishable from a well-behaved client on a quiet
cluster.

## On the achieved rate

Each publish is awaited before the next is offered, so the rate this reaches is lower
than the rate asked for. That is the right trade for what this example is for: a drill
asks whether the counters moved, not how fast, and one confirm at a time is what makes
`confirmed` a statement about the broker rather than about a buffer.

## Why a drill cannot use a probe instead

A probe that connects to the broker answers "is the cluster usable". It cannot answer
whether the application was told the broker had stopped reading from it, whether it
stopped publishing, or whether it recovered on its own — and those differ between client
libraries that are otherwise equivalent. The only thing that can report them is a
client.

## Also see

- `advanced/04-blocked-broker` — the same condition asserted rather than reported,
  including what health says about it.
- `advanced/03-observability` — these counters exposed to a scraper instead of a log.
