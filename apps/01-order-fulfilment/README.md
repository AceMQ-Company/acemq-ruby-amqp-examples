# apps/01 — order fulfilment (microservices)

Five services, one broker, no shared database, and no service that knows another
exists.

Everything in `basic`, `intermediate` and `advanced` demonstrates one idea at a
time. This is what they look like when they have to coexist: an outbox at the
edge, idempotency where double-charging is real harm, a retry ladder where a
downstream is flaky, and one correlation id that turns five services into one
story.

It is the Ruby port of Java's
[`apps/01-order-fulfilment`](https://github.com/AceMQ-Company/acemq-java-amqp-examples/tree/main/apps/01-order-fulfilment):
the same five services, the same exchange, queues and routing keys, the same
event names and the same camelCase payloads, and the same four orders checked
the same way.

## The flow

```mermaid
flowchart LR
    C["customer"] --> G["gateway<br/>orders + outbox<br/>one transaction"]
    G -->|order.placed| P["payments<br/>idempotent charge"]
    P -->|payment.captured| I["inventory<br/>retry ladder"]
    P -->|payment.declined| N
    I -->|stock.reserved| S["shipping"]
    I -->|stock.unavailable| N
    S -->|order.shipped| N["notifications<br/>fulfilment.#"]
```

Each service owns one decision and publishes what happened. None of them calls
another.

## What each service is here to show

| Service | The pattern | Why it lives there |
|---|---|---|
| [gateway](gateway.rb) | Transactional outbox (`Patterns::SQLOutboxStore`) | The edge is where the dual-write problem lives: save the order *and* announce it, or a crash loses one of them |
| [payments](payments.rb) | Shared idempotency store (`Patterns::SQLIdempotencyStore`) | The only service where handling a message twice is real money. Claims before charging, confirms after publishing |
| [inventory](inventory.rb) | Retry policy, and an outcome that is not a failure | Tells "the warehouse timed out" (retry) from "there are three left and they want ten" (publish it, never retry) |
| [shipping](shipping.rb) | Nothing clever | The point: it reacts to one event, does one thing, publishes one event. Adding a service beside it changes nothing |
| [notifications](notifications.rb) | Topic wildcard | Bound to `fulfilment.#`. Added without touching a single publisher, and the next one will be too |

[contracts.rb](contracts.rb) is the only file every service requires: the
events, the exchange, the queues and the topology. No domain model, no database,
no helpers — a contracts file that grows those has become a shared library.

## Running it

```bash
docker compose up -d --wait
bundle install
bundle exec ruby apps/01-order-fulfilment/main.rb
```

[main.rb](main.rb) starts all five services in one process against a real
RabbitMQ and puts four orders through, each into a freshly started system with
fresh databases: one that succeeds, one where the warehouse is flaky, one over
the payment limit, and one where stock runs out. It checks every claim it makes,
exits non-zero on the first that does not hold, and finishes in a couple of
seconds.

**On a broker somebody else is using, give it a vhost of its own.** It deletes
and redeclares the `fulfilment.*` queues on start, because whatever an earlier
run left behind would otherwise land in this run's counters — and the other
four languages' ports of this app use exactly the same names:

```bash
ACEMQ_URL=amqp://guest:guest@localhost:5672/ruby-apps bundle exec ruby apps/01-order-fulfilment/main.rb
```

## Design decisions worth arguing with

**A database per service.** The moment two services read the same table, the
deployment boundary is fiction. The gateway and payments each get their own
SQLite file, in a directory that lives for one scenario.

**The relay has its own database connection.** It runs on its own thread, and
one SQLite handle shared with the request thread would put the relay's
mark-as-published inside whatever transaction the request had open. The
connections are opened in WAL mode with `busy_handler_timeout`, not
`busy_timeout`: the latter holds Ruby's global lock while it waits, so the
thread holding the database lock cannot run to release it.

**Every service applies the whole topology on start-up.** Applying it five
times is safe, and it means no service depends on another having started
first — there is no deployment order to get wrong.

**Payments claims by order id, not by message id.** The library's
`Patterns.idempotent` keys on the message id by default, which catches a broker
redelivery. Keying on the order also catches the same order arriving as a
different message, which is the duplicate that costs money.

**Payments runs before inventory.** Reserving stock for an order that cannot be
paid for is how a warehouse fills with holds nobody releases.

**Money is taken before stock is confirmed available.** When stock runs out the
customer has already been charged, and the run checks exactly that. A real
system triggers a refund here; the example leaves it visible rather than
pretending the problem does not exist. That compensation is what
[a saga](../../intermediate/06-saga) would add.

**The retries wait in the consumer, not in the broker.** The policy is four
attempts from 0.2 seconds to 5, and every one of those is under the
thirty-second threshold above which a wait is moved into a broker rung queue.
Java's default threshold is also thirty seconds, and its inventory service uses
the same delays. A warehouse that is down for minutes rather than milliseconds
is when the rungs earn their place.

## The correlation id is the whole observability story

```ruby
@mq.publish(event.to_wire, to: routing_key, exchange: EXCHANGE,
                           type: event.class.type, correlation_id: cause.correlation_id)
```

Every service copies it forward. Notifications rebuilds the customer's timeline
from nothing but that id:

```
OrderPlaced -> PaymentCaptured -> StockReserved -> OrderShipped
```

Four services that never spoke to each other, assembled into one sequence. Drop
the `correlation_id:` in any one of them and the order vanishes from the
timeline — the run then fails waiting for the fourth event, which is the check
doing its job — and so do your traces and your log correlation in production.

## What the Ruby run checks that Java's does not

The same assertions as Java's system test, and three more, because each is a
way the library could lose or duplicate a message that no other check would
see:

- **The relay published each order once.** Payments counts the duplicates it
  refuses, and the first scenario requires none.
- **Nothing was set aside.** Every `.dlq` and `.parked` queue is asked for its
  depth at the end. A message given up on fails no other check — the order just
  quietly is not where it was expected.
- **The retry count is the library's own**, read off its telemetry
  (`acemq.messages.retried.total`), rather than counted by the handler. Java
  reads `consumer.retried()`; a Ruby consumer has no counter of its own, and the
  metric is the same number under the name every AceMQ library reports it as.

## What is deliberately not here

No HTTP. The gateway exposes `place_order` as a method, because adding a web
framework would triple the code and demonstrate nothing about messaging. In a
real service that method body is the handler behind a POST.

No compensation. See above — the refund path is named and not implemented.

No claim that a Ruby service and a Java one interoperate *in this app*. The
names and payload shapes are identical by construction, and the envelope
headers are the library's shared wire format, but this app has not been run
with the two languages' services mixed on one broker.

## Related

- [intermediate/03-outbox](../../intermediate/03-outbox) — the outbox on its own
- [intermediate/02-idempotent-consumer](../../intermediate/02-idempotent-consumer) — one message delivered four times and charged once
- [basic/02-retries-and-dead-letters](../../basic/02-retries-and-dead-letters) — the retry ladder
- [advanced/03-observability](../../advanced/03-observability) — the trace this correlation id enables
