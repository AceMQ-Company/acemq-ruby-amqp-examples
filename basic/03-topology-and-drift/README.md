# basic/03 — topology, and a broker that disagrees

A topology described once, printed so it can be read before it is applied, then
applied — and a second service that declares the same queue differently being
told so.

## What it shows

- **A topology is data before it is an effect.** `puts topology` prints the
  declarations in the order a broker needs them, which is worth putting in a
  deployment log: reading it is cheaper than working out afterwards what a
  service did to a shared vhost.
- **Invalid is caught before anything is declared.** A binding to a queue the
  topology never declares is a mistake with a name, and `problems` gives it
  one.
- **A queue's kind is part of its identity.** Redeclaring a quorum queue as
  classic is `PRECONDITION_FAILED`, and that refusal is passed on rather than
  swallowed.

## Running it

```bash
docker compose up -d --wait
bundle exec ruby basic/03-topology-and-drift/main.rb
```

## What to look for

```
Topology: 2 exchanges, 3 queues, 2 bindings
  declare exchange shipping-events (topic)
  declare exchange acemq.dlx (direct)
  declare queue shipping.labels (quorum, durable, x-dead-letter-exchange=acemq.dlx, x-dead-letter-routing-key=shipping.labels.dlq)
  declare queue shipping.labels.dlq (classic, durable)
  declare queue shipping.scratch (classic, durable)
  bind shipping.labels.dlq to acemq.dlx on "shipping.labels.dlq"
  bind shipping.labels to shipping-events on "label.#"

applied.

the broker refused the second declaration:
  PRECONDITION_FAILED - inequivalent arg 'x-queue-type' for queue 'shipping.labels' in vhost '/': received 'classic' but current is 'quorum'
```

Three things in that plan were never asked for. `acemq.dlx`,
`shipping.labels.dlq` and the binding between them came from
`dead_letter: true`, because a dead-letter queue with no exchange reaching it
is a queue nothing can be delivered to.

`shipping.labels` is **quorum** and `shipping.labels.dlq` is **classic**, and
neither was stated. A source queue is quorum by default — the same default as
`declareQueue` in Java, so a Java producer and a Ruby consumer declare the same
thing — and it is replicated, so it survives losing the node its leader was on.
Dead-letter queues and retry rungs stay classic because Java declares them
classic, and a rung's whole behaviour is a time to live expiring into an
exchange, which is the plainest thing a classic queue does.

`shipping.scratch` is classic because it was asked for, and saying so in the
topology is how the next person knows it was a decision rather than an
accident.

## The plan is intent, not a diff

It is deliberately **not** a comparison against the live broker. AMQP offers no
way to enumerate what is there without the management API, and a plan that
quietly guessed would be worse than one that is honest about being a statement
of intent.

## Why the refusal matters

`x-queue-type` is part of a queue's identity to the broker. A Java service
declaring `shipping.labels` as quorum and a Ruby service declaring it as
classic do not negotiate: whichever starts second is answered
`PRECONDITION_FAILED` and consumes nothing at all. The library passes that
refusal on rather than swallowing it, because it means this service and the
broker disagree about what the queue is, and carrying on would leave the
disagreement in place with nobody told.

**A queue that already exists as classic cannot be redeclared as quorum.** The
broker refuses, and there is no conversion — drain it and recreate it, or keep
it classic explicitly with `queue_type: :classic` until you can.
