# AceMQ for Ruby — examples

[![ci](https://github.com/AceMQ-Company/acemq-ruby-amqp-examples/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-ruby-amqp-examples/actions/workflows/ci.yml)
[![authorship guard](https://github.com/AceMQ-Company/acemq-ruby-amqp-examples/actions/workflows/attribution-guard.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-ruby-amqp-examples/actions/workflows/attribution-guard.yml)
[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-3.1%2B-CC342D)](#requirements)

Runnable examples for [AceMQ for Ruby](https://github.com/AceMQ-Company/acemq-ruby-amqp).
Each one is a single `main.rb`: open a directory and the whole example is in
front of you, with no shared helpers to trace.

Each one also **checks what it claimed** and exits non-zero when it did not —
the attempt counter reached three, the duplicate was charged once, the delayed
message actually waited, the blocked connection still reported itself up. So a
run of this repository is twenty-one small integration tests that happen to be
readable, rather than twenty-one scripts that print something and succeed.

## Running one

```bash
docker compose up -d --wait
bundle install
bundle exec ruby basic/01-publish-and-consume/main.rb
```

Point them somewhere else with `ACEMQ_URL`:

```bash
ACEMQ_URL=amqp://guest:guest@broker:5672 bundle exec ruby basic/01-publish-and-consume/main.rb
```

**No trailing slash.** bunny reads `amqp://host:port/` as a request for the
vhost named by the empty string and answers `NOT_ALLOWED - vhost  not found`,
which is a confusing way to spend twenty minutes.

**Delete `Gemfile.lock` before you install.** No lock file is committed — the
Gemfile says why — so `bundle install` resolves the newest 0.x the feed carries
every time. A lock left behind by an earlier run pins whatever it named instead,
and `bundle install` honours it and reports success, so the suite runs against
an old library and says nothing. `rm -f Gemfile.lock` first, or run
`./etc/tls-broker.sh`, which does it for you.

One example needs a broker with a TLS listener, and TLS needs certificates that
cannot be committed. `./etc/tls-broker.sh` writes them with the library's own
generator and brings the broker up from them.

**[04-blocked-broker](advanced/04-blocked-broker) has a broker to itself**, on
5673, and `docker compose up -d --wait` brings it up with the rest. It raises a
genuine memory alarm with `rabbitmqctl set_vm_memory_high_watermark 0`, and an
alarm is broker-wide: on the shared broker it would stop every other example
publishing as well. It puts the watermark back in an `ensure`.

## What is here

### basic

| | |
|---|---|
| [01-publish-and-consume](basic/01-publish-and-consume) | A durable quorum queue, a published message, and a consumer that says what it read. |
| [02-retries-and-dead-letters](basic/02-retries-and-dead-letters) | The attempt counter moving, a message giving up, and a failure marked fatal skipping the wait. |
| [03-topology-and-drift](basic/03-topology-and-drift) | A topology printed before it is applied, and a broker refusing a service that disagrees about a queue. |
| [04-codecs](basic/04-codecs) | JSON, YAML, TOML and XML read off one queue, which is what a format migration looks like. |
| [05-replay](basic/05-replay) | Dead letters put back once the fix is out — some of them, in stages, on attempt one. |
| [06-streams](basic/06-streams) | Six readings written once and read three times, from three different places. |
| [07-pipelines](basic/07-pipelines) | An order carried through three services by an itinerary, and a failed run resumed where it stopped. |

### intermediate

| | |
|---|---|
| [01-request-reply](intermediate/01-request-reply) | Ten concurrent requests, each getting its own answer, and a responder failure reaching the caller. |
| [02-idempotent-consumer](intermediate/02-idempotent-consumer) | One logical message delivered four times and charged once. |
| [03-outbox](intermediate/03-outbox) | A message written in the same transaction as the work, and a relay publishing what committed. |
| [04-interceptors](intermediate/04-interceptors) | A tenant stamped on every message and every handler timed, without either appearing in the handler. |
| [05-binary-codecs](intermediate/05-binary-codecs) | Protobuf and Avro on one queue, and a schema that changed underneath a consumer without breaking it. |
| [06-saga](intermediate/06-saga) | Three steps that changed the world, a fourth that failed, and the first three undone in reverse. |
| [07-scheduling](intermediate/07-scheduling) | A message delivered later, with no scheduler process and no plugin. |
| [08-claim-check](intermediate/08-claim-check) | Half a megabyte put in a store and 39 bytes on the wire, with the small invoice still travelling whole. |
| [09-consumer-groups](intermediate/09-consumer-groups) | Four slow invoices, handled four times faster by four consumers than by one. |
| [10-schema-evolution](intermediate/10-schema-evolution) | Two services on two versions of one schema, talking to each other anyway. |

### advanced

| | |
|---|---|
| [01-encrypting-payloads](advanced/01-encrypting-payloads) | Message bodies the broker cannot read, and a keyring that rotates without a flag day. |
| [02-development-certificates](advanced/02-development-certificates) | TLS against a private authority, and the marker that stops a development certificate reaching production. |
| [03-observability](advanced/03-observability) | Prometheus metrics, a health report that proves a round trip, and spans that join across the broker. |
| [04-blocked-broker](advanced/04-blocked-broker) | A real memory alarm, and a connection that reports `up` in microseconds rather than `down` in seconds. |

## The three worth reading even if you never run them

**[02-retries-and-dead-letters](basic/02-retries-and-dead-letters)** prints
`[1, 1, 2, 3]`. The attempt counter travels on the message rather than being
kept in the consumer, because a count kept in the consumer is wrong the moment
a second consumer exists. Reading it off the publisher's envelope instead would
print `[1, 1, 1, 1]`, and a retry limit built on that would never trip.

**[03-topology-and-drift](basic/03-topology-and-drift)** ends with a
`PRECONDITION_FAILED`. A queue's type is part of its identity to the broker, so
two services that disagree about it do not negotiate — whichever starts second
consumes nothing at all. Every AceMQ library declares the same table for
exactly this reason.

**[07-scheduling](intermediate/07-scheduling)** asks for three seconds and gets
two. The cost of not requiring a broker plugin is that delivery is accurate to
about the smallest rung, and an example that hid that would be selling
something.

## How the library is resolved

`acemq-amqp` is not on rubygems.org. It is published to AceMQ's own static feed
at <https://acemq.org/gems>, which is a directory tree over HTTPS and needs no
account and no credential — one `source` line is the whole of what a user does.

The [Gemfile](Gemfile) resolves `acemq-amqp` from that feed, `~> 0.7`, so every
example here runs against exactly what the documentation tells you to depend
on. For a while it could not: the published 0.3.0 predated the sagas, the
scheduler, the YAML, TOML, XML, Protobuf and Avro codecs, the encrypted bodies,
the development certificates and the OpenTelemetry adapter — more than half of
what is demonstrated here — so the Gemfile resolved the library's `main` branch
instead, because an example that cannot be run is worth very little. 0.5.0
carries all of it, and that arrangement is over.

The `published-gem` job in CI installs the gem the other way round, with
`gem install --source https://acemq.org/gems` rather than through Bundler.
The two read a static feed differently, and it is the command the library's
README gives a reader, so it is worth running even though the examples already
prove Bundler's path.

`ACEMQ_RUBY_AMQP=../acemq-ruby-amqp bundle install` points it at a local
checkout instead, which is how a change to the library is tried against these
examples before it is pushed anywhere.

**There is no `Gemfile.lock` here, on purpose.** A lock would pin the exact
version of the library and of everything under it, and CI would then run that
resolution for ever — so a release that broke an example, or a dependency that
raised its Ruby floor, would go unnoticed until somebody happened to unpin.
That is exactly the drift these examples exist to catch. Resolving afresh is
what makes a Monday build tell us something. The cost is that pins a lock would
have supplied have to be written down in the Gemfile instead, and two of them
are.

## Requirements

Ruby 3.1 or newer, and Docker. CI runs everything on **3.1** specifically,
because that is the floor the library promises and the version that catches a
dependency quietly raising its own — `opentelemetry-api` did at 1.9,
`google-protobuf` at 4.36, and `multi_json` at 1.20. The conditional pins in
the Gemfile are what that costs, and they say so.

## How these stay honest

CI **runs every one of them against a real broker**, on every push and once a
week, and lints them with zero offences allowed. Examples rot: the library
moves on, the example does not, and a newcomer's first experience is a stack
trace. Running them here turns that into a red build instead.

The workflow finds examples rather than listing them, so one added without
touching CI is still run — and it fails if it finds fewer than it expects,
since a `find` that matches nothing would otherwise pass having run nothing at
all. It also fails on an example directory with no `README.md`, because an
example nobody can read is an example nobody runs.

## Licence

Apache 2.0. See [LICENSE](LICENSE).
