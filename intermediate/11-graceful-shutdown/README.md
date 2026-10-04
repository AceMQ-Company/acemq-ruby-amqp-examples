# Graceful shutdown

A shutdown that waits for the handler in hand — and, in 0.7.5, goes on waiting
past the deadline it was given.

```bash
bundle exec ruby intermediate/11-graceful-shutdown/main.rb
```

## What to look for

```
enough time  close took 1.5s and returned
not enough   close(timeout: 0.5) took 1.5s and returned

what the handlers got to:
  J-1  started
  J-1  finished
  J-2  started
  J-2  finished

left on the queue for the next reader: nothing
```

**`enough time … close took 1.5s and returned`.** `Connection#close` is not a
socket close with a consumer attached. It cancels every subscription first, so
nothing new is delivered, then waits for the handlers already running, and only
then shuts the socket. J-1 was being worked on when close was called, and it was
finished and acknowledged rather than handed back to the broker for somebody else
to redo. That is the half of a graceful shutdown that works as documented.

**`not enough … close(timeout: 0.5) took 1.5s and returned`.** The other half,
and the line that is wrong. A deadline shorter than the handler is supposed to
end the wait: close stops waiting, shuts the socket anyway, and raises
`Connection::DrainTimeout` afterwards, whose `stranded` says how many deliveries
each queue was left holding. Here close waited the whole job out, J-2 finished,
and nothing was raised.

## Why the deadline is not honoured in 0.7.5

The drain stops a consumer with bunny's `basic_cancel`. When the last consumer on
a channel is cancelled, bunny shuts that channel's consumer work pool and **waits
for the busy worker** — up to the pool's shutdown timeout, which the library
leaves at bunny's default of sixty seconds. That wait happens inside the cancel,
before the library's own deadline is consulted, so in practice:

- **A handler shorter than sixty seconds is always waited for**, whatever
  `timeout:` says. `close(timeout: 0)`, documented as not waiting at all, waits
  too.
- **The wait is per consumer.** Several busy consumers cost up to sixty seconds
  each, one after the other — the four-minute shutdown that the single shared
  deadline was written to prevent.
- **Past sixty seconds** close does raise `DrainTimeout` and the job is
  redelivered, but the handler is not stopped: bunny has already given up on its
  pool, so the thread runs on, orphaned, until it finishes or the process exits.
  Measured against 0.7.5 with a 63-second job and `timeout: 0.5`: close took
  60.5s, raised, and the job came back with `redelivered=true`.

With Kubernetes' default thirty-second grace period that means a slow handler is
SIGKILLed in the middle of a close that was asked to give up at twenty. The job
is still redelivered, because it was never acknowledged — so nothing is lost —
but the `DrainTimeout` an operator would alert on is never raised.

**The example asserts what 0.7.5 does.** When a release makes the drain honour
its deadline, the second close starts raising and this example fails on purpose,
saying so. It then wants rewriting to show the outcome the API promises — the
exception, its `stranded` count, and J-2 coming back redelivered — rather than
going on describing a library that no longer exists.

## The shape a service wants anyway

```ruby
stop = Thread::Queue.new
Signal.trap("TERM") { stop << :term }
stop.pop
begin
  mq.close                      # timeout: Connection::DRAIN_TIMEOUT, 20
rescue Connection::DrainTimeout => e
  logger.warn(e.message)
end
```

The default is twenty because the number that matters is the one that will kill
the process: Kubernetes waits `terminationGracePeriodSeconds`, thirty unless
changed, before SIGKILL. Twenty inside thirty leaves room for the web server and
for the process to exit, so the drain loses the race to your own log line rather
than to the orchestrator. Until the deadline is honoured, keep handlers well under
the grace period — that is the bound actually in force.

`DrainTimeout` is raised rather than logged because a drain that reports success
having abandoned work is how an operator whose grace period is too short never
finds out. It is the line worth alerting on.

## Redelivery is the backstop

Whatever a handler was holding when the process died was never acknowledged, so
the broker gives it to the next reader. The work may be done twice, which is the
ordinary at-least-once case and costs nothing when the handler is idempotent —
[`intermediate/02`](../02-idempotent-consumer) is that argument. A graceful
shutdown reduces duplicates; it does not eliminate them. A power cut has no
SIGTERM.

## Compared with Java

Java has `close()`, which does not wait, and `drain(timeout)`, which returns
whether the handlers finished. Ruby has one `close` that always drains, and an
exception when the drain fell short. Both leave an abandoned handler running in
the process; in a real pod it dies with the pod.
