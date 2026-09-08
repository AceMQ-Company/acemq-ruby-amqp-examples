# intermediate/06 — sagas

Two bookings arrive as messages. One goes through; the other fails at the last
step, and the three that already happened are undone in reverse.

## What it shows

- **A step knows how to undo itself**, and compensation runs in the order the
  world was changed in, backwards.
- **A step with no compensation is skipped rather than refused.** One that only
  read something needs no undoing.
- **`run` returns a result rather than raising**, because a failed saga is not
  an exceptional condition to a caller that has to decide what happens next.
- **`unresolved` is the field to alert on.**

## Running it

```bash
docker compose up -d --wait
bundle exec ruby intermediate/06-saga/main.rb
```

## What to look for

```
B-1  confirmed    failed_at=nil unresolved=[]
B-2  compensated  failed_at="book-hotel" unresolved=[]

what happened, in order:
  charged 480
  reserved seat on BA117
  passport checked
  booked a hotel in Lisbon
  charged 990
  reserved seat on FI451
  passport checked
  released seat on FI451
  refunded 990
```

The last two lines are the example. The seat is released **before** the payment
is refunded, because that is the reverse of the order they happened in.
`check-passport` has no compensation and is simply skipped.

## What it is not

**Not a distributed transaction.** After `take-payment` the money has really
moved. The refund is a new fact rather than an erasure of the old one, which is
why the result reports what was compensated instead of pretending nothing
happened.

**Not durable.** A crash midway leaves it half-applied with nothing to resume
it. If that matters, the state has to live somewhere that survives the process
— which is a different design, and one this library does not pretend to
provide.

## `unresolved` is the alert

When a compensation itself fails, the remaining ones still run: stopping would
leave more undone than continuing. The step that failed to undo goes into
`unresolved`, and that set is real-world effects that happened, were meant to
be undone, and were not. Nothing else in the system knows about them, no retry
will resolve them, and a person has to.

Everything else a saga reports is recoverable by construction. This is the one
line worth paging somebody about.

## Why this example has a broker in it

Nothing in `Patterns::Saga` touches one. It is on a consumer here because that
is where a saga usually runs: the request arrives as a message, the steps are
calls to other systems, and the outcome is published as another message. The
`causation_id` on the outcome records which booking produced it.
