# Graceful shutdown

A shutdown that waits for the handler in hand, and gives up at its deadline.

```bash
bundle exec ruby intermediate/11-graceful-shutdown/main.rb
```

## What to look for

```
enough time   close took 3.00s and returned
not enough    close(timeout: 0.5) took 0.51s and raised
              the drain did not finish within 0.5s: 1 delivery was left unsettled and will be redelivered — rb-shutdown-jobs (1)
              {"job":"J-2"} came back, redelivered=true
three busy    close(timeout: 0.5) took 0.50s and raised
              the drain did not finish within 0.5s: 3 deliveries were left unsettled and will be redelivered — rb-shutdown-jobs (3)
              {"job":"J-3"} came back, redelivered=true
              {"job":"J-4"} came back, redelivered=true
              {"job":"J-5"} came back, redelivered=true

what the handlers got to:
  J-1  started
  J-1  finished
  J-2  started
  J-3  started
  J-4  started
  J-5  started
  J-2  finished
  J-3  finished
  J-4  finished
  J-5  finished

left on the queue: 0, dead-lettered: 0
```

**`enough time … close took 3.00s and returned`.** `Connection#close` is not a
socket close with a consumer attached. It cancels every subscription first, so
nothing new is delivered, then waits for the handlers already running, and only
then shuts the socket. J-1 was being worked on when close was called, and it was
finished and acknowledged rather than handed back to the broker for somebody else
to redo. The default deadline is twenty seconds; a three-second job fits.

**`not enough … took 0.51s and raised`.** A deadline shorter than the handler
ends the wait. Close stops waiting, shuts the socket anyway, and raises
`Connection::DrainTimeout` afterwards. Its `stranded` is a count per queue —
`{"rb-shutdown-jobs" => 1}` here — and its `timeout` is the deadline that
expired. J-2 was never acknowledged, so the broker hands it to the next reader
with `redelivered=true`.

**`three busy … took 0.50s and raised`.** Three consumers on one connection, each
in the middle of a job. Every one is stopped first and then all are waited for
against **one** deadline, so close takes half a second, not three halves. A wait
spent per consumer is not a bound a process can be held to: eight consumers at
twenty seconds each is nearly three minutes, and no orchestrator waits that long.

**`J-2 finished` after the close returned.** A handler still running at the
deadline is not killed — Ruby has no safe way to stop a thread mid-transaction —
so it runs to the end in this process. What it returns is thrown away: no ack,
retry, dead letter or park, because the broker already has the message back and
settling it as well would be the same message twice. Hence nothing left on the
queue and nothing dead-lettered. Deliveries bunny had received but no handler
had started are not started once the drain begins; they go back with the
channel.

The example checks each of these claims itself and exits non-zero, naming the
one that broke, if any does not hold.

Up to 0.7.5 the deadline was not honoured against a real broker: bunny waited for
the busy handler, for up to sixty seconds per consumer, before the drain looked
at its clock.

## The shape a service wants

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
than to the orchestrator. Raise it for handlers that genuinely take longer, and
raise the grace period with it.

`DrainTimeout` is raised rather than logged because a drain that reports success
having abandoned work is how an operator whose grace period is too short never
finds out. It is the line worth alerting on.

## Redelivery is the backstop

Whatever a handler was holding when the deadline passed, or when the process
died, was never acknowledged, so the broker gives it to the next reader. The work
may be done twice, which is the ordinary at-least-once case and costs nothing
when the handler is idempotent — [`intermediate/02`](../02-idempotent-consumer)
is that argument. A graceful shutdown reduces duplicates; it does not eliminate
them. A power cut has no SIGTERM.

## Compared with Java

Java has `close()`, which does not wait, and `drain(timeout)`, which returns
whether the handlers finished. Ruby has one `close` that always drains, and an
exception when the drain fell short. Both leave an abandoned handler running in
the process; in a real pod it dies with the pod.
