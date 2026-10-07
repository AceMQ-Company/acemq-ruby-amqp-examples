# apps/02 — policy administration (modular monolith)

One process, six modules, one database, and no module that requires another.

[apps/01](../01-order-fulfilment) is five services that cannot call each other
because a network is in the way. This is the same discipline with the network
removed: the modules run in one process, share one connection and one database,
and still communicate only by publishing events. The boundary is which files a
module requires — [contracts.rb](contracts.rb) and nothing else — which is
weaker than a deployment against a determined engineer and strong enough against
a distracted one.

**A modular monolith is not a step towards microservices.** It is a different
answer to the same question, and for most organisations the better one: module
boundaries without distributed transactions, independent reasoning without
independent deployment, and one database you can actually join across.

It is the Ruby port of Java's
[`apps/02-policy-administration`](https://github.com/AceMQ-Company/acemq-java-amqp-examples/tree/main/apps/02-policy-administration):
the same exchange, queues, routing keys and pipeline, the same event names and
camelCase payloads, and Java's five system tests as five scenarios with the same
checks.

## The flow

```mermaid
flowchart LR
    B["broker submits"] --> P["policies<br/>applications + outbox<br/>one transaction"]
    P -->|application.submitted| U["underwriting<br/>pipeline: register → price → decide"]
    U -->|application.accepted| P
    U -->|application.declined| A
    P -->|policy.issued| BI["billing<br/>idempotent premium"]
    P -->|policy.issued| C["claims"]
    C -.->|"asks: is it in force?"| P
    D["documents<br/>claim check"] -->|document.stored| A["audit<br/>policy.#"]
    BI -->|premium.charged| A
```

The dotted line is the only one that is not an event: claims **asks** policies a
question and waits for the answer.

## What each module is here to show

| Module | The pattern | Why it lives there |
|---|---|---|
| [policies](policies.rb) | Transactional outbox (`Patterns::SQLOutboxStore`) | One database does *not* remove the dual write. The two systems that must agree are this database and the broker, and no transaction spans both |
| [underwriting](underwriting.rb) | `Patterns::Pipeline` | The one genuinely sequential part: check the register, price it, decide. A queue per stage, so a slow stage is a deep queue you can point at, and only the register stage has a retry ladder |
| [documents](documents.rb) | Claim check | A scanned medical report is tens of megabytes. The store gets the bytes; the message gets the key |
| [billing](billing.rb) | Shared idempotency store (`Patterns.idempotent` over `SQLIdempotencyStore`) | The only module where handling a message twice is money |
| [claims](claims.rb) | Request/reply (`Patterns::Requester`, `Patterns.serve`) | Needs an answer *now*, before settling. Asks over the broker even though the callee is in the same process |
| audit | Topic wildcard | A queue bound to `policy.#`. Every event, including ones not invented yet |

## The outbox is still necessary

This surprises people, so it is the first thing to read:

```ruby
@db.transaction do
  @db.execute("INSERT INTO applications VALUES (?, ?, ?, ?, ?)", [...])  # this database
  @outbox.add(Patterns.record(@mq, event.to_wire, ...), connection: @db) # the same transaction
end                                                                      # one decision, both writes
```

A monolith removes the distributed transaction *between modules*. It does nothing
about the one between a module and its broker. Save the application and publish
the event without an outbox, and a crash between them still loses one of the two.

## Why claims asks instead of reading

```ruby
@requester = Patterns::Requester.new(mq, to: POLICY_LOOKUP, timeout: 5)
status = PolicyStatus.from_wire(@requester.call(PolicyQuery.new(policy_id: id).to_wire))
```

The moment claims calls into policies directly, the two are one module and no
file layout will separate them again. Asking over the broker costs a millisecond
and keeps the seam. When the lookup times out the claim is **neither settled nor
rejected**: a lookup that did not answer is not a "no".

## Running it

```bash
docker compose up -d --wait
bundle install
bundle exec ruby apps/02-policy-administration/main.rb
```

[main.rb](main.rb) is the only file that requires more than one module. It puts
the application through Java's five scenarios, each into a freshly started
application with a fresh SQLite database: an ordinary application that becomes a
policy and is charged once; one above the automatic limit that is referred and
never becomes a policy; a claim settled and a claim rejected on the strength of
an answer from policies; a four-megabyte document that travels as a key; and
three copies of one event that charge once beside the original. It checks every
claim, exits non-zero on the first that does not hold, and finishes in a few
seconds.

**On a broker somebody else is using, give it a vhost of its own.** It deletes
and redeclares the `policy.*` and `underwriting.*` queues on start, because
whatever an earlier run left behind would land in this run's counters, and the
other languages' ports use exactly the same names:

```bash
ACEMQ_URL=amqp://guest:guest@localhost:5672/ruby-apps bundle exec ruby apps/02-policy-administration/main.rb
```

## What the Ruby run checks that Java's does not

- **The audit queue holds every event exactly once** — 25 across the five
  scenarios. Nothing consumes it, so its depth is a count of what the exchange
  received: one fewer is a message lost on the way, one more is a duplicate the
  relay or a retry introduced. Java's tests check the modules' counters, which a
  lost `PremiumCharged` would not move.
- **The pipeline finished its run**, read off the library's own
  `acemq.pipeline.run.total{outcome="completed"}` rather than this application's
  counters.
- **No dead-letter or parking queue holds anything**, including the three
  pipeline stages'.

## Where Ruby differs, and why it matters

**Every publish here says `mandatory: true`.** Java's publishes are mandatory by
default; Ruby's, like Go's and Python's, are not. Java's README records what
writing this application found: `claim.settled` and `document.stored` were
published with nothing bound to them, and Java refused the publish — "nothing is
bound to exchange 'policy' for routing key 'policy.claim.settled'". In Ruby,
without the flag, both would have been confirmed and dropped, and the bug found
weeks later by someone asking where the audit trail went. The `audit` queue
exists because of that failure, and so does the flag.

**The outbox relay used to lose exactly that kind of message.** It is not the
caller's publish, so the caller cannot pass the flag, and until the fix in
`acemq-ruby-amqp` (unreleased at the time of writing; 0.7.10 still has it) it
published without it: a record nothing was bound to was confirmed, dropped and
marked published. Writing this application is what found it. Every record here
is routable, so the run passes on 0.7.10 either way; the fix keeps an unroutable
record in the outbox, counts the attempt, and tells `on_error:` — what Java's
and .NET's relays already did.

**Ruby's `Pipeline` is a list of step names.** Java's builder carries a
description and a retry policy per step. Here the retry policy goes on the
step's consumer, which is the same thing, and the descriptions live in
[underwriting.rb](underwriting.rb) and are printed in the start-up line Java
logs. The step names, the exchange (`underwriting`, direct) and the queues
(`underwriting.register`, `.price`, `.decide`) are Java's exactly. The last
step returns what it decided on rather than `nil`: in Ruby a step that returns
`nil` *ends the run early*, a filter's answer, and is counted apart from a run
that finished.

**One database, opened per thread.** Java hands each module a `DataSource`.
Ruby has no such thing for SQLite, so `main.rb` hands each module a lambda that
opens a handle, and each module opens one per thread that uses it: policies for
its own reads and writes (behind a mutex, because SQLite allows one transaction
per handle), its relay, and billing's idempotency store.

Interop with Java's modules is by design — same names, same payloads, same
queue types — but has not been run mixed.

## What is not here yet

The claim check is a Hash in [documents.rb](documents.rb), not a library type,
as it is in Java. **Retention is the part to think about before you ship one.**
The store and the queue have different lifetimes, and a message replayed a month
later carries a key the store may have expired — worse than a lost message,
because it looks like a message.

## Related

- [apps/01-order-fulfilment](../01-order-fulfilment) — the same patterns across five processes
- [apps/03-ledger](../03-ledger) — events as the system of record rather than notifications about one
