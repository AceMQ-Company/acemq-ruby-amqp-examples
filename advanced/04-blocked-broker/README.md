# advanced/04 — a blocked broker

A real memory alarm on a real broker, and the health report that comes back in
microseconds saying the connection is blocked rather than down.

## What it shows

- **A blocked connection is `:up`**, with the broker's own reason on it — not
  `:degraded`, which is what the same connection reported up to 0.6.0.
- **The round trip is skipped while it is blocked**, so the answer arrives in
  under a tenth of a millisecond instead of hanging until bunny gives up.
- **The publish that was waiting is confirmed afterwards.** A block pauses
  publishing; it does not lose anything.

## Running it

This example has a broker to itself, on 5673, because the alarm it raises is
broker-wide:

```bash
docker compose up -d --wait
bundle exec ruby advanced/04-blocked-broker/main.rb
```

`ACEMQ_BLOCKED_URL` points it somewhere else, and `ACEMQ_BLOCKED_CONTAINER`
names the container the alarm is raised in — it reaches the broker with
`rabbitmqctl`, because nothing an AMQP client sends can raise or clear an alarm.

## What to look for

```
before the alarm: up in 4160us
  consumers: 0
  consumers_running: 0
  queues: []
  round_trip_ms: 4

setting the memory high watermark to 0 on acemq-ruby-examples-blocked-broker
blocked: 0 publishes confirmed since the alarm, and one still waiting
while blocked: up in 97us
  the broker has blocked this connection; publishing is paused: low on memory
  consumers: 0
  consumers_running: 0
  queues: []
  blocked: true
  blocked_reason: "low on memory"

putting the watermark back to 0.6
1 publish made it through in the end, including the one that waited
after the alarm cleared: up in 10259us
  consumers: 0
  consumers_running: 0
  queues: []
  round_trip_ms: 10
```

## Why `:up` and not `:down`

A blocked connection is the broker protecting itself from a producer that is
doing nothing wrong. A service told to fail its own readiness check for it is one
an orchestrator restarts into the same blocked broker, having thrown away
whatever it was holding — and doing that to every replica at once turns a broker
under memory pressure into an outage with a crash loop on top.

The state still has to be **visible**, so it goes on an `:up` report:
`parts["blocked"]` is `true`, `parts["blocked_reason"]` is what the broker said,
and the detail leads with `Health::BLOCKED` — the same sentence in Python, Java,
Go and .NET, so one alert rule matches a blocked broker whatever the service
happens to be written in. The example compares against the constant rather than
against a copy of the words, because it is a contract and not a phrasing.

Blocking never moves the status downwards. A report that was `:degraded` because
a consumer had stopped stays `:degraded` and says both things.

## Why the timing is part of the claim

`mq.health` normally declares a queue and deletes it again, because a declare is
the cheapest thing AMQP offers that actually proves the round trip. A blocked
broker will not complete one — and it does not fail it either. It hangs, until
bunny's continuation timeout gives up seconds later and reports `:down` for a
broker that is up and talking, which would overrule every careful word above at
exactly the moment they matter.

So the block is read first, off the notification the broker already sent over the
same socket, and the declare is skipped. `round_trip_ms` is absent from the parts
while blocked, because nothing was timed — and that absence is one of the things
this example checks, since it is the shape a regression here would take.

## Restoring the watermark

The watermark is read off the broker before the alarm and put back in an
`ensure`. Reading it matters: RabbitMQ 3.13 defaults to `0.4` and 4.x to `0.6`,
so a number written into the file would be wrong against one of them. The
`ensure` matters because every check in this example is about a broker under an
alarm, which makes every one of them a way to leave with the alarm still on — and
the next thing to use the broker would find it blocked for a reason nothing in
its own output explains.

## The publish runs on a thread

Not for concurrency. A publish on a blocked connection does not raise — it sits
in `wait_for_confirms` until the broker starts reading the socket again. That is
the state being demonstrated, so the main thread cannot be the one waiting on it.
The thread is stopped and joined once the alarm is off, before anything else
touches the connection.
