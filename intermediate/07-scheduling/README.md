# intermediate/07 — delivering a message later

Three reminders: one already overdue, one in three seconds, one in five. No
scheduler process, no plugin, no cron.

## What it shows

- **A ladder of queues, each with a uniform time to live.** A message hops
  through them until it is due.
- **The accuracy that buys, and what it costs.**
- **The payload is encoded once and carried as bytes**, with its content type
  in a header and put back on the message finally delivered.

## Running it

```bash
docker compose up -d --wait
bundle exec ruby intermediate/07-scheduling/main.rb
```

It takes about five seconds, because it is waiting for real delays.

## What to look for

```
scheduled 3, delivered 3, hops 6

asked for   arrived at
     past      0.0s   R-0
       3s      2.0s   R-1
       5s      4.0s   R-2

content type on arrival: application/json
```

**Read the two columns together.** A three-second delay lands at about two.
A message is delivered as soon as less than one second is left, because another
hop through the smallest rung would cost more than the accuracy it buys.

That is the trade this design makes, and it is stated rather than hidden:
delivery is accurate to about the smallest rung. Something that must fire at
09:00:00.000 wants a scheduler, not a message broker.

**`hops 6`** is the other number. R-0 was due already and took none; the other
two took three each. A one-day delay costs twenty-four hops and a one-minute
delay costs one — long delays are several broker round trips rather than one,
which is the honest cost of not requiring a plugin.

## Why not a per-message time to live

Because a classic queue expires messages only from its **head**. Put a
four-hour message in and a one-minute message behind it, and the one-minute
message is delivered in four hours — and nothing reports it. The queue looks
healthy, the message is not lost, it is simply late by a factor nobody
predicted.

It is the single most common way a home-made scheduler fails, and it fails in
production under mixed load rather than in testing under uniform load.

The ladder avoids it by giving every message in a rung the same delay, so the
head is always the message due soonest.

## The names are the contract

`acemq.schedule.{1h,10m,1m,10s,1s}`, `acemq.schedule.due`, and the four headers
a scheduled message carries are shared with the Java, Go, .NET and Python
libraries. Two services scheduling on one broker declare the same queues, and a
rung declared with a different time to live is a `PRECONDITION_FAILED` for
whichever declares second. Nothing about it is a local decision.

`Scheduler.declare` is public so a deployment that applies its topology up
front can include the scheduler's without starting a consumer.

## Blocks and lifetimes

`Scheduler.on(mq) { |scheduler| ... }` closes the scheduler on the way out,
which is what a script that schedules and exits wants. This example does not
use the block form, because the scheduler has to still be running when the
messages come due — a scheduler that has been closed is a ladder nobody is
watching.
