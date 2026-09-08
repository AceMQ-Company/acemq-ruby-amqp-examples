# intermediate/01 — request and reply

Ten requests in flight at once, each getting its own answer, and a responder
failure reaching the caller as an exception rather than as a timeout.

## What it shows

- **A `Requester` is kept and reused.** It holds a reply queue and a consumer,
  so one per request means a queue per request.
- **Correlation pairs a reply with its request**, not arrival order. Ten
  threads calling at once get ten right answers.
- **The responder returns the answer, not an `Ack`.** Raising sends the failure
  back to whoever is waiting.

## Running it

```bash
docker compose up -d --wait
bundle exec ruby intermediate/01-request-reply/main.rb
```

## What to look for

```
reply queue: acemq-reply-c52fa180-822d-43ce-8d16-dfde56646dc8
  request  1  X-1 -> 1299
  request  2  X-2 -> 450
  ...
the responder's failure reached the caller: the responder failed: RuntimeError: no such product: NOPE
```

**The failure is the interesting line.** A responder that returned an error
object would need every caller to remember to check it; one that stayed silent
would make the caller wait out its ten-second timeout to learn something the
responder knew immediately. Having answered, the request is settled rather than
retried — replying and then retrying would answer twice.

## This is a synchronous shape on an asynchronous system

That is a real cost rather than a free convenience. A caller blocked on a reply
is holding a thread, a connection and a deadline, and a responder that backs up
turns into a caller that stops responding. Reach for it where the caller
genuinely cannot go on without the answer, and publish an event otherwise.

**A timeout says an answer did not arrive.** It says nothing about whether the
work was done, which is why a request that changes anything wants an idempotent
responder — see [02](../02-idempotent-consumer).

## The reply queue

Without `reply_to:` the requester generates one: exclusive, transient,
auto-deleting, and gone when this process is. A reply queue that outlived its
requester would collect answers nobody is waiting for.

That one is **classic, necessarily** — RabbitMQ replicates nothing that
disappears with its connection, so asking for a quorum queue that is also
exclusive is a contradiction the library refuses before the broker does. A
named `reply_to:` queue is an ordinary durable queue and gets the ordinary
quorum default, so naming a queue a topology also declares is safe.

## Two application headers

`acemq-reply-to` and `acemq-error` carry this, and they are **application**
headers on purpose. The `x-acemq-` namespace belongs to the engine and is kept
away from what a handler sees, so a responder could never read them if they
lived there.
