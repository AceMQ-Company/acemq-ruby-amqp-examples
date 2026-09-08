# basic/01 — publish and consume

The smallest thing worth running: a topic exchange, a durable quorum queue, a
message published to it, and a consumer that says what it read. Everything else
in this repository is this with one more idea added.

## What it shows

- **A topology is declared before anything is published.** Exchange, queue,
  binding, in the order a broker needs them.
- **`dead_letter: true` declares `orders.new.dlq` alongside the queue.**
  Nothing here uses it. A queue with nowhere to put a message it cannot handle
  is a queue that drops one, and the time to arrange that is before the first
  failure rather than during it.
- **Every handler returns an `Ack`.** `accept` means the message has been dealt
  with and can be removed.
- **The envelope survives the trip.** The type, the identifier, the origin and
  the attempt counter all come back on the other side, and they are what
  another AceMQ library in another language reads.

## Running it

```bash
docker compose up -d --wait
bundle exec ruby basic/01-publish-and-consume/main.rb
```

## What to look for

```
payload      {"order_id" => "A-1", "total" => 4299}
type         order.placed.v2
id           483e47ae-94f4-4181-9b55-0bb84ece0d02
correlation  483e47ae-94f4-4181-9b55-0bb84ece0d02
origin       examples/01-publish-and-consume
attempt      1
routing key  order.placed
```

**The correlation identifier is the message identifier.** Nothing set it: it
defaults to the id so that the next message in a chain has something to copy
rather than a decision to make. A chain of five messages started from this one
all carry this identifier, which is what makes them one thing in a log.

**The attempt is 1**, and it is on the message rather than counted here — see
[02](../02-retries-and-dead-letters) for why that matters.

**`origin` was set once, on the connection.** It costs nothing and it is the
difference between a dead-lettered message you can trace to a process and one
you cannot.

## The polling at the bottom

Handlers run on the transport's threads, so what they see has to cross back to
this one, and a `Thread::Queue` is the plainest way to do it. It is polled with
a deadline rather than blocked on because `Thread::Queue#pop` only grew a
timeout in Ruby 3.2, and this library promises 3.1 — and because an example
that fails with a readable message beats one that hangs until CI kills it.
