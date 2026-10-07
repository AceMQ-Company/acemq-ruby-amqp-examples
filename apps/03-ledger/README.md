# apps/03 — event-sourced ledger

The log **is** the system of record. Balances are not stored; they are what you
get by adding up the log, and can be thrown away and rebuilt at any time.

[apps/01](../01-order-fulfilment) and [apps/02](../02-policy-administration)
publish events describing what happened to a system of record that lives in a
database. Here there is no such database. Every entry is appended to a stream and
nothing is ever updated or deleted — money moved wrongly is corrected by posting
the opposite entry, exactly as a paper ledger does, and both entries stay. That
is what lets a ledger answer *"what did we believe on Tuesday"*.

It is the Ruby port of Java's
[`apps/03-ledger`](https://github.com/AceMQ-Company/acemq-java-amqp-examples/tree/main/apps/03-ledger):
the same stream, queues, exchange and routing keys, the same camelCase payloads
with amounts in whole minor units, and Java's five system tests as five
scenarios with the same checks.

## Why a stream and not a queue

**A queue is emptied by being read. A stream is not.** That single difference is
the reason this application uses one:

- the writer reads the whole journal at start-up to recompute balances;
- a statement projection reads the same journal, from the same offset, at the
  same time, and neither reader affects the other;
- a projection written next year starts at offset zero and gets all of history.

Note the two together: commands go to an ordinary queue (`ledger.commands`),
entries go to a stream (`ledger.journal`). A command must be applied once; a fact
is for everybody.

## The modules

| Module | |
|---|---|
| [ledger](ledger.rb) | The only writer. Decides whether a transfer is allowed and appends the entries. Rebuilds its balances from the journal on start-up, with `Patterns.read_stream(..., offset: StreamOffset.first)` |
| [projections](projections.rb) | A statement per account, built by reading from offset zero. Stores nothing the log does not contain |
| [transfers](transfers.rb) | Where transfers are asked for, as commands, and refusals noticed |

## One writer, deliberately

Every transfer produces two entries that sum to zero. Two processes appending
independently cannot enforce that — a stream will happily accept an unbalanced
pair from each. Making the writer singular is what makes the invariant checkable,
and what makes the writer's own running totals correct after the rebuild.

**Read to the end, then stop.** Java's first version kept following the stream
*and* applied each entry as the writer wrote it, so every entry was counted
twice. The rebuild here reads until the journal has been quiet for 0.4 seconds,
then cancels its reader, and the writer keeps its totals from there.

## Running it

```bash
docker compose up -d --wait
bundle install
bundle exec ruby apps/03-ledger/main.rb
```

Streams need no plugin: `x-queue-type: stream` is core since RabbitMQ 3.9 and
reachable over AMQP 0-9-1, so this runs on the same broker as everything else.

[main.rb](main.rb) runs Java's five scenarios, each against a freshly started
ledger — which rebuilds from the journal every time, so every scenario after the
first begins by reading back what the earlier ones wrote: a transfer posting two
entries that sum to zero; a transfer that would overdraw, refused and recorded;
a projection from offset zero agreeing with the writer; a projection started
later that still sees all of history; and two readers of one stream that do not
compete. It deletes the journal once at the start so a run is repeatable — the
only time anything is deleted from it.

## What the Ruby run checks that Java's does not

Java's tests start a new ledger per test, which rebuilds from the journal, but
never assert on accounts from an earlier test. This run ends with **a sixth
writer started from nothing but the journal**, and checks:

- every one of the ten accounts holds what the five scenarios left it;
- the journal holds exactly 15 entries — five openings and two per transfer that
  went through, none for the refusal. One fewer is an entry lost, one more an
  entry appended twice;
- the entries sum to the money that was put in;
- no dead-letter queue holds anything.

## Where Ruby differs

**Publishes say `mandatory: true`**, as Java's do by default; Ruby's do not. An
entry the broker could not route is an entry the log does not have, and
without the flag it would be confirmed and dropped.

**The rebuild's "caught up" is a quiet period, as in Java's.** The precise way
is to read the offset of the last entry before starting and stop there; neither
version does, and both say so.

Interop with Java's modules is by design — same stream, same names, same
payloads — but has not been run mixed.

## What is honestly not here

- **Snapshots.** A rebuild is O(history). The answer is "the balance at offset N,
  plus everything after N", deliberately omitted.
- **Atomic double entry.** The two halves of a transfer are appended one after
  the other; a real ledger appends them as one record so a crash between them is
  impossible.
- **Retention.** The journal keeps an hour, because this is an example. **If
  retention is shorter than "forever", the projection is the system of record
  after all** — and nobody wrote that down.

## Related

- [basic/06-streams](../../basic/06-streams) — offsets and replay, one idea at a time
- [apps/02-policy-administration](../02-policy-administration) — events about a system of record, rather than the record itself
