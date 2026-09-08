# basic/05 — replay

A dead-letter queue with six invoices on it, of which three were broken by one
tenant's currency converter. The fix goes out, and they go back through — some
of them, in stages, with a count of what moved and what did not.

This is the thing somebody actually does at three in the morning.

## What it shows

- **The block decides which messages go.** Everything it declines stays where
  it is, so a replay can be done a tenant or a failure mode at a time rather
  than all at once.
- **`limit:` and the reason.** `moved 2, skipped 0 (limit)` means something
  quite different from `moved 2, skipped 0 (drained)`, and the result says
  which.
- **A replayed message goes back on attempt one**, with `x-acemq-error`
  cleared.

## Running it

```bash
docker compose up -d --wait
bundle exec ruby basic/05-replay/main.rb
```

## What to look for

```
dead letters after the outage: 3
first pass:  moved 2, skipped 0 (limit)
second pass: moved 1, skipped 0 (drained)
  I-1  attempt 1  replayed from invoices.issued.dlq
  I-3  attempt 1  replayed from invoices.issued.dlq
  I-5  attempt 1  replayed from invoices.issued.dlq
```

**Attempt 1.** That is the part worth checking. A message dead-lettered on the
last attempt of a five-attempt policy would arrive back on attempt five, the
consumer would give up on it before the handler ever ran, and two thousand
messages would move from the dead-letter queue to the dead-letter queue with
nothing visible from outside. `restart: false` puts back exactly what was there
— for an audit, or for a queue read by something that counts attempts itself.

The identity is untouched either way: same id, same correlation, same
`x-acemq-first-seen`. So giving up on **age** still applies, which is right —
the fix was for the bug, not for the clock.

Each message is stamped with `acemq-replayed-from`, `acemq-replayed-at` and
`acemq-replay-count`, so a consumer that needs to treat replays differently
can, and one that does not is unaffected.

## Why `routing_key:` is given

A dead letter's routing key is the dead-letter queue — the consumer put it
there by name. Replaying through the default exchange without overriding that
would read a message and write it straight back onto the queue it came from,
for ever, and the only sign would be a queue that never empties. The library
refuses that combination outright rather than defending against it with a
limit, which would turn an infinite loop into a finite one that still did
nothing.

## Why declined messages are held

Messages the block declines are held **unacknowledged** for the length of the
pass rather than returned one at a time. Returning one immediately does not
work: the broker puts it back at the head of the queue, so the next read hands
over the same message and everything behind it is never seen. The broker still
holds them, so a tool that dies half way through returns them rather than
losing them.

A message is acknowledged only after the broker has confirmed the new copy, so
a crash in that gap replays it twice — which is the right way round for a
dead-letter queue.
