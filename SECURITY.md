# Reporting a vulnerability

Email **security@acemq.com** with what you found and how to reproduce it. Please do
not open a public issue for anything exploitable.

You should get an acknowledgement within two working days, and an assessment of
whether it is a vulnerability, what is affected, and a rough timeline within a week.
If a fix is warranted, we will tell you when it is released and credit you unless you
would rather we did not.

## What is in scope

This repository is example code, and most of what is worth reporting about it is a
vulnerability in the [library](https://github.com/AceMQ-Company/acemq-ruby-amqp) that
an example happens to demonstrate. Report it there, or here, and it will end up in
the right place.

Things worth reporting even if they feel minor:

- A way to reach a broker without the certificate verification the configuration
  asked for.
- A development certificate accepted without `allowing_development_certificates`.
- Anything that renders a credential, a key, or a message body into a log or an
  exception message.
- A message body that can be altered without `EncryptedCodec` refusing it.
- A YAML or XML body that reaches a parser able to construct objects, read files or
  expand entities.
- An example that teaches a practice which is unsafe in production, which is a real
  defect here even though nothing in this repository runs anywhere.

## What is not

- **The examples using `guest:guest` against a local broker.** They talk to a
  container on loopback that is thrown away afterwards.
- **`etc/tls-broker.sh` writing private keys into `certs/`.** They are
  thirty-day development certificates, every one of them carries
  `ACEMQ DEVELOPMENT ONLY - DO NOT TRUST`, the library refuses them unless told
  otherwise, and the directory is in `.gitignore`.
- **`Security.without_verifying_the_broker` accepting any certificate.** That is what
  it is for, it says so at length, and development certificates are refused on top.
- **Vulnerabilities in RabbitMQ itself** — report those to Broadcom.
- Findings from a scanner with no demonstrated impact.

## Supported versions

This repository has no releases. `main` is what is supported, and a fix is a commit
on it.

## What the library does not do for you

It carries messages, secures the connection, and — if asked — the message body. It
does not manage broker users or permissions, hold your keys, or authenticate
anything that serves the metrics. The
[security guide](https://acemq.org/acemq-ruby-amqp/security.html)
says which is which.
