# advanced/02 — TLS and development certificates

A TLS connection to a broker with its own certificate authority, the generator
that makes such a broker easy to stand up, and the refusal that stops those
certificates reaching production.

## What it shows

- **`Security.verified(certificate_authority:)` narrows trust to that authority
  alone.** The system trust store is not consulted at all.
- **A development certificate is refused before a socket is opened** — and
  saying so is a separate, visible step rather than a keyword.
- **`Credentials` keeps the password out of the URL** and renders as
  `[REDACTED]`.
- **What the generator writes**, and that it is short-lived and mode 0600.

## Running it

This one needs a broker with a TLS listener, which needs certificates, which
the library writes:

```bash
./etc/tls-broker.sh
bundle exec ruby advanced/02-development-certificates/main.rb
```

`etc/tls-broker.sh` generates the certificates into `certs/` with
`DevelopmentCertificates.generate`, then brings up `compose.yaml` with the
`tls` profile — the broker reads a `rabbitmq.conf` the generator wrote, so the
paths cannot disagree. CI runs the same script.

Point it elsewhere with `ACEMQ_TLS_URL` and `ACEMQ_TLS_CA`.

## What to look for

```
credentials:  #<AceMQ::AMQP::Credentials username="guest" secret=[REDACTED]>

refused, before a socket was opened:
  the certificate authority certs/ca.crt carries "ACEMQ DEVELOPMENT ONLY - DO NOT TRUST". ...

over TLS:     {"sent_at" => "2026-09-08T23:09:50Z"}

the generator writes:
  ca.crt  ca.key  client.crt  client.key  rabbitmq.conf  server.crt  server.key
expires:      2026-10-08 23:09:50 UTC
authority:    /O=ACEMQ DEVELOPMENT ONLY - DO NOT TRUST/CN=AceMQ development CA
ca.key mode:  0600
```

The refusal and the connection use the **same certificate authority**. The only
difference between them is one call:

```ruby
Security.verified(certificate_authority: CA).allowing_development_certificates
```

## Why a development certificate is refused at all

A self-signed authority that drifts into production is **worse than no
encryption**, because everything looks protected and nothing is verified.
Somebody points a staging config at production, the connection opens, the
dashboard says TLS, and the trust anchor is a file anybody can regenerate.

Every certificate the generator writes carries
`ACEMQ DEVELOPMENT ONLY - DO NOT TRUST` in its subject organisation, and both
halves are checked: an authority or client certificate configured in this
process is refused when the connection is made, and one the broker presents is
refused during the handshake — **however trust is configured, unverified mode
included**. Java, Go and .NET refuse the same marker.

Allowing them is a separate line and not a keyword for the same reason
`without_verifying_the_broker` has a long name: it has to be legible in a diff.
It weakens nothing else — verification stays on, and a certificate that does
not verify is still refused.

## Why `Security` is a class and not three keyword arguments

**bunny does not verify the broker's certificate when it is given a URL.** Not
"verifies weakly" — does not verify. A URL string is parsed by `AMQ::Settings`,
which merges in its own defaults, one of which is `verify: false`; bunny reads
that as an explicit instruction and sets `VERIFY_NONE`. So
`Bunny.new("amqps://broker:5671")` encrypts the traffic, accepts a certificate
the connecting process could have made up thirty seconds ago, and reports
itself as `tls?` throughout. Nothing warns, because from bunny's side somebody
asked for this.

Every mode in `Security` therefore states `verify_peer` outright rather than
leaving it unsaid. The same reach lifts bunny's TLS ceiling, which it otherwise
pins to 1.2 even where both ends could have agreed on 1.3.

## Why the password is not in the URL

A URL is the one piece of configuration that gets printed — into error
messages, structured logs, `ps` output, whatever the deployment tool echoes
back — and a password that has been through any of those has to be rotated.
Passed separately it never takes the trip, and the object renders as
`[REDACTED]` through `inspect`, `to_s` and `%p` alike.

`Credentials.from_file` reads a mounted Kubernetes or Docker secret, and
passing a block instead of an object defers the read to connection time, which
is what a secret rotated underneath a running process needs.

## The warning bunny prints

```
WARN -- Using TLS but no client certificate is provided.
```

That is bunny noticing this connection authenticates by password. The generated
`rabbitmq.conf` sets `verify_peer` with `fail_if_no_peer_cert` off, so the
broker will use a client certificate if one is offered and will not insist —
which is what suits a development broker that is also reached by password. For
a broker that authenticates by certificate, hand `Security.verified` the
client's `certificate:` and `key:` as well; they go together, and one without
the other is refused where it is configured rather than at the handshake,
because a handshake failure names neither file.
