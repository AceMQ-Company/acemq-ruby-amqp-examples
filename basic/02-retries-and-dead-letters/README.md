# basic/02 — retries and dead letters

Two charges: one whose acquirer keeps timing out, and one on a card that has
been reported stolen. The first is retried until the policy runs out; the
second gives up immediately. Both end on the dead-letter queue, with the reason
written down.

## What it shows

- **The attempt counter moves, and it moves on the message.** `x-acemq-attempt`
  travels with the delivery rather than being counted in the consumer, because
  a count kept in the consumer is wrong the moment a second consumer exists —
  a message moving between them would be on attempt one for ever — and a
  restart forgets it anyway. The trade is that a retried message goes to the
  back of its queue rather than the front.
- **`FatalError` skips the remaining attempts.** It is how a handler says "this
  will not work next time either" without having to know how many attempts are
  left.
- **A dead letter carries why.** `rejected by the handler: …` and `gave up
  after 3 attempts: …` are different sentences and both travel on the message.
  That is the value of dead-lettering rather than answering the broker with a
  bare `basic.reject`, which drops the message somewhere with nothing at all
  saying what happened to it. `Ack.reject` is not that: it is a handler saying
  "this one is not processable", and it writes its reason down.

## Running it

```bash
docker compose up -d --wait
bundle exec ruby basic/02-retries-and-dead-letters/main.rb
```

## What to look for

```
deliveries   [1, 1, 2, 3]
schedule     [0.2, 0.4]  (seconds, before jitter)
dead letter  attempt 1: rejected by the handler: AceMQ::AMQP::FatalError: the card was reported stolen
dead letter  attempt 3: gave up after 3 attempts: the acquirer timed out
```

`[1, 1, 2, 3]` is the whole example. Three of those are the timing-out charge
counting up; the fourth is the stolen card, delivered once. Reading the counter
off the publisher's envelope instead would print `[1, 1, 1, 1]`, and a retry
limit built on that would never trip.

## Where the wait happens

The delays here are 200ms and 400ms, which is under the library's 30-second
threshold, so **the consumer waits them out itself**, holding one prefetch
slot. At or above the threshold the message is published into a rung queue —
`payments.charges.retry.40s` and so on — whose time to live is the delay and
whose dead-letter target is the queue it came from, so the broker returns it
when the time is up with nothing running.

The line is there because a consumer that sleeps through a five-minute backoff
loses the whole wait when it restarts: the broker redelivers at once, which is
a correctness bug rather than a throughput one. Below the threshold a lost wait
costs seconds, and a queue per rung of a schedule that finishes in the time it
takes to notice is not worth what it costs the broker.

The topology declares the rungs anyway — `queue(..., retry_policy: POLICY)`
works out which ones the policy needs. They cost nothing unused, and a rung
discovered one failure at a time is a rung that was missing when it mattered.
`acemq.retry.rung.missing` in the metrics is worth an alert for exactly that.

## Why the failure is acknowledged

When the policy is out of attempts the message is republished to
`payments.charges.dlq` and the **original is then acknowledged**. That looks
wrong and is what makes it reliable: the message is already safely somewhere
else, so the original is a copy that has been dealt with. Rejecting it instead
would either requeue it into a hot loop or hand it to whatever dead-lettering
the queue happens to carry — and neither of those can write down *why*, which
is the one thing whoever finds it needs.
