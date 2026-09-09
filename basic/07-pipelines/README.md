# Pipelines

An order carried through three services by an itinerary it brings with it — and
a failed run put back where it stopped rather than at the beginning.

```bash
bundle exec ruby basic/07-pipelines/main.rb
```

## What to look for

**`visited: ["validate", "charge", "ship"]`.** No step knows what comes next.
Each one does its job and returns the payload; `follow_slip` reads the slip off
the message, advances it, and publishes to whatever is now at the front. The
order of the steps is a property of the message, so changing it is a deploy of
the thing that starts runs and not of the three services doing the work.

**`shipped: {"order" => "A-1", "stamps" => [...]}`.** What a step returns is what
the next one receives, which is the difference between a pipeline and three
consumers that happen to publish to each other. Returning `nil` instead ends the
run there — a step that decides a message goes no further is making a decision,
not failing, and it is counted apart from both.

**`ran: ["validate", "charge"]`, then `["validate", "charge", "charge",
"ship"]`.** This is the whole example. The card issuer refuses, `charge` raises
`FatalError`, and the run stops — with a message on
`rb-fulfilment.charge.dlq` that still carries the route and how far along it was.
Putting it back runs `charge` again and `validate` **not at all**. A restart
would have run both, and `validate` is the cheap half: the expensive one is a
step that already moved money, sent an email or called somebody else's API.

**`at position 1 of validate,charge,ship`.** The two halves of a resume. The
route says what the steps are; the position says which one is next. Everything
needed to resume is on the message that failed:

```ruby
envelope = Envelope.from_headers(stopped.headers, stopped.routing_key)
resume   = Patterns::RoutingSlip.from(envelope, pipeline: PIPELINE)
resume.start(mq, JSON.parse(stopped.body))
```

## Two wire forms, and why there are two

Every AceMQ library reads both.

**The JSON itinerary**, in the `acemq-routing-slip` header, assembled per message
with `RoutingSlip#step`:

```json
{"steps": [{"exchange": "", "routingKey": "rb-slip-charge", "name": "charge"}],
 "done":  [{"exchange": "", "routingKey": "rb-slip-validate", "name": "validate", "completedAt": "…"}]}
```

Each step names its own destination, so a consumer can follow it knowing nothing
in advance. That is what makes it right for a route assembled per message — a
refund that skips a step, an order that needs an extra approval.

**The declared route**, which is what `Patterns::Pipeline` writes and what Java
has. Three short reserved headers — `x-acemq-route`, `x-acemq-route-position`,
`x-acemq-route-id` — carrying the step *names* and nothing else, resolved by the
consumer against a pipeline it declared. It is smaller on the wire, it reads in a
management console without decoding anything, and one run can be followed across
every hop by its `route-id` — at the price of a route that has to be declared on
both ends and is the same for every message.

`Pipeline` also owns the naming, and it is Java's exactly: the exchange is the
pipeline's own name and is direct, the routing key is the step name, and the
queue behind a step is `pipeline.step`. `pipeline.topology` is that as a
`Topology` to apply, so it composes with everything else a service declares.

`RoutingSlip.from` reads either form, and `follow_slip` writes back the one that
arrived unless told otherwise (`write:`). That default is what lets a Ruby step
sit in the middle of a pipeline a Java service declared: answering a declared
route with a JSON slip would hand the next Java step a message with no route on
it, and the run would stop half way with nothing saying why.

`pipeline:` is needed only for the declared form, because the pipeline's own name
is the one thing those headers do not carry. `PIPELINE.follow(mq)` is
`Patterns.follow_slip(mq, pipeline: PIPELINE)` with the argument already filled
in.

## The rule a step has to keep

`follow_slip` accepts the incoming message **only once the next one is out**, so
a failure to publish retries the step that just succeeded. Every step that
changes anything therefore has to be idempotent — the same requirement
[`intermediate/02-idempotent-consumer`](../../intermediate/02-idempotent-consumer)
exists to meet, and it is not optional here.

Raising `FatalError` stops the run where it is and skips the retries, which is
what happens at `charge`. An ordinary exception is retried by the connection's
policy first, and only then dead-lettered.

## What a declared pipeline counts

Finishing a run through a `Pipeline` raises `acemq.pipeline.run.total`, tagged
with the pipeline's name and the outcome, and observes the whole run's duration —
the age of the envelope, which was created when the message entered and carried
through every hop. A bare JSON slip raises neither: it is an itinerary somebody
assembled for one message, not a thing with an identity to put on a dashboard.
