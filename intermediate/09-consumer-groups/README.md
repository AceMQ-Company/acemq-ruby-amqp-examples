# Consumer groups

Four slow invoices, handled four times faster by four consumers than by one.

```bash
bundle exec ruby intermediate/09-consumer-groups/main.rb
```

## What to look for

```
                              at once  seconds
group of 4, prefetch 1              4      0.5
1 consumer, concurrency 4           1      2.0
```

**Both rows are running four handlers.** The difference is how many messages the
broker will hand over before it hears back. A prefetch belongs to a *consumer*,
not to a process: one consumer with `concurrency: 4` and a prefetch of one holds
one message, three of its four handlers sitting idle with nothing to work on.
Four consumers have four prefetches, so all four messages are out at once and the
batch takes as long as one invoice rather than four.

That is the whole of what a group buys that `concurrency:` does not, and the
example asserts it both ways: it fails if the group does not reach four in
flight, and it fails if the single consumer ever reaches two.

**`still running after close: 0`.** The other half. `group.close` stops every
consumer and waits for the handlers already running — and stops *every* one even
when one of them refuses, raising the first refusal afterwards, because leaving
three running after a shutdown the caller believes happened is worse than the
failure that started it. Four consumers started by hand are four things to
remember to stop.

**A group is sized from a number.** `size:` is an argument, which is the value
most often changed after a service is already running and the most awkward one to
change when the consumers are four lines of start-up code.

## When `concurrency:` is the right answer anyway

When the handlers are waiting on something rather than doing something, and one
channel's prefetch is nowhere near the limit. `concurrency: 8` on one consumer
with a prefetch of 32 is cheaper than eight consumers: one channel, one set of
broker-side bookkeeping. Raise the prefetch first; reach for a group when the
handlers are slow enough that the prefetch is what is holding things up, or when
a fair share **across processes** matters — the broker round-robins between
consumers, so four here compete evenly with four in another instance, where one
consumer with `concurrency: 4` would take a quarter of what four take.

## What the broker sees

Each consumer in the group is tagged `acemq-{queue}-{n}`, numbered from one. In
the management interface that is four named rows on the queue rather than four
identical ones, so "which consumer is holding that message" has an answer.

## A group is not a partition

Every consumer here reads the same queue and the broker decides who gets what, so
two messages about the same order can be handled at the same moment by different
consumers. If that matters, a group is the wrong tool and the answer is a routing
key per key.

## Why the counter has a lock

Handlers run on the transport's threads, not the main one, so the "how many at
once" counter in this example is guarded by a `Mutex`. That is not a detail of
the pattern — it is what every handler in this repository that touches shared
state has to do, and a group makes it four times as likely to matter.
