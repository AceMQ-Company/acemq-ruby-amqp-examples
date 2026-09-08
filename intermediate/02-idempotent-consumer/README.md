# intermediate/02 — an idempotent consumer

One logical message delivered four times, and one charge.

## What it shows

- **`Patterns.idempotent` wraps a handler and hands back a handler.** It goes
  to `consume` unchanged, so the retry policy, the dead-lettering and the
  envelope are all still whatever the connection was configured with. A pattern
  that took over the consumer would have to reimplement them, and then there
  would be two retry engines to keep in step.
- **A duplicate is accepted, not rejected.** The work was done, so the message
  has been handled.
- **The message id is the default key**, because it is what a broker
  redelivers unchanged.

## Running it

```bash
docker compose up -d --wait
bundle exec ruby intermediate/02-idempotent-consumer/main.rb
```

## What to look for

```
delivered  4
charged    1 (["C-9"])

the three duplicates were accepted rather than dead-lettered:
  billing.charges.dlq holds 0
```

Four deliveries and one charge is the claim. The empty dead-letter queue is the
other half of it: dead-lettering a duplicate would raise an alarm about
something that went right, and somebody would spend a morning on a queue full
of successes.

A handler that does *not* accept has its key forgotten, so its retry can
actually run — otherwise the first failure would poison the message for the
length of the window.

## The store in this example is the wrong one

`InMemoryIdempotencyStore` is right behind one worker and wrong the moment
there are two: each process has its own memory, so both are told they are
first. It is here because it makes the example one file.

The store worth having is your own database, written **in the same transaction
as the work**. That is also the only arrangement that closes the gap between
the handler finishing and the acknowledgement reaching the broker — which is
why this is a guard against duplicates rather than exactly-once, and why no
library can close it on your behalf. `Patterns::SQLIdempotencyStore` is the
one that takes your connection; it holds a key under a lease rather than a
lock, so a consumer that died mid-handler has not silently deleted a message.

## Keying on something other than the id

`key:` takes the key from the payload instead, for when two genuinely different
messages carry the same order and doing that order twice is the thing to
prevent. A message that produces an empty key is dead-lettered rather than
retried: the key function will produce the same nothing next time, and a guard
that cannot key a message is not guarding it.
