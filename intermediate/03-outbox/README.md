# intermediate/03 — the transactional outbox

Two accounts opened: one that commits and one that is rolled back by a
compliance check. One message is published.

## What it shows

- **The message is written in the same transaction as the work.** Both commit
  or neither does.
- **The relay publishes afterwards**, out of the committed record.
- **A record holds encoded bytes**, not an object, because it outlives the
  process that wrote it and the class may not survive the deployment that
  happens while it waits.

## Running it

```bash
docker compose up -d --wait
bundle exec ruby intermediate/03-outbox/main.rb
```

## What to look for

```
rolled back: the compliance check failed
accounts committed: ["A-1"]
outbox holds:       1
relay published:    1
arrived:            {"account_id" => "A-1"}
type:               account.opened.v1
```

**`outbox holds: 1` after two `add` calls** is the whole example. A-2's message
went away with A-2's account, because they were one write.

A service that writes to a database and then publishes has two things that can
fail independently. Crash between them and the work is committed with nobody
told; publish first and then fail to commit, and the world has been told about
something that did not happen. The second is worse and the first is more
common, and both disappear when the message is part of the transaction.

## Why the store is written out here

`Patterns::InMemoryOutboxStore` exists and works, and it is not what this
example uses. It has its own memory — `add` cannot join a transaction it knows
nothing about — so it would demonstrate the shape of the pattern while quietly
not having the property the pattern is for.

The `Ledger` class in `main.rb` is thirty lines and does have it. A store is
anything answering `add`, `pending` and `mark_published`; there is no base
class to inherit and nothing to register. The real one is
`Patterns::SQLOutboxStore`, which takes **your** connection and writes on it,
inside **your** transaction, and neither commits it nor closes it.

That is the whole point of the design. A store that opened its own connection
would commit the insert on its own, and a business write that rolled back
afterwards would leave a message queued for something that never happened —
the exact fault the pattern was adopted to prevent, now harder to notice
because the code looks right.

## The relay is at-least-once, deliberately

A record is removed only after the broker has confirmed it, so a crash in
between sends the message again. Anything consuming an outbox needs to be
idempotent, which is why [02](../02-idempotent-consumer) is in the same
library. Removing first would lose messages instead, and an absence cannot be
recognised the way a duplicate can.

`sweep` is public so an application can flush at the end of a request rather
than up to an interval later — and so this example does not have to sleep. In a
service you would `relay.start` and leave it running.

## It is indistinguishable on the wire

The record's envelope is built by the same rules `publish` uses, down to the
origin, so a message that went through the outbox looks exactly like one that
did not. A consumer cannot tell, and should not need to.
