# Schema evolution and the registry

Two services on two versions of one schema, talking to each other anyway.

```bash
bundle exec ruby intermediate/10-schema-evolution/main.rb
```

## What to look for

```
reader   written with  decoded
v1       id 1          {"order_id" => "A-1", "total" => 42.0}
v1       id 2          {"order_id" => "B-2", "total" => 99.5}
v2       id 1          {"order_id" => "A-1", "total" => 42.0, "currency" => "EUR"}
v2       id 2          {"order_id" => "B-2", "total" => 99.5, "currency" => "GBP"}
```

**Row two is the one that matters.** A producer already on the new schema, a
consumer still on the old one. The `currency` field the writer added is *skipped*
— not misread, and not left shifting every field after it, which is what happens
to a decoder that assumes the writer used the schema the reader holds. Nobody had
to redeploy the old consumer to make the new producer safe to ship, and that is
the only reason to run a registry.

[`intermediate/05-binary-codecs`](../05-binary-codecs) shows row three — a reader
ahead of its writer, filling `currency` in from the default — as one consequence
of choosing a codec by content type. This example is about the registry itself
and about row two, which is the direction that reaches production first.

**`written with id 1` / `id 2` is read off the wire.** Every registered message
carries the identifier of the schema it was *written* with, framed on the front:
one zero byte, four bytes of identifier, big-endian, then the Avro body. That is
Confluent's framing, and Java, Go, .NET and Python write the same five bytes. The
identifier is the only thing that makes any of the rest work — Avro resolves a
*writer* schema onto a *reader* schema, and without it a reader has no idea what
the writer used.

**`a reader with the wrong registry: … no schema with id 2`.** Ruby's registered
codec asks the registry for an identifier the moment it meets one it does not
know, so a consumer deployed after the producer needs no priming and no restart.
Pointed at a registry that has never heard of the schema, it says so — a
consumer that cannot resolve a writer schema has a real problem, usually a
producer registered against a different registry, and carrying on with the
reader's own shape would turn that into silently wrong data downstream.

**Registering the same definition twice does not make a v3.** A schema is
identified by a SHA-256 of its exact bytes, so the same definition comes back
with the same id. Without that, a service that registers its schemas on every
start adds a version per restart and the version number stops meaning anything.
Two definitions differing only in whitespace *are* two schemas, which is strict
on purpose: normalising first would need a parser per format, and a registry that
treated two schemas as one because it mis-parsed them would be worse than one
that is merely fussy.

**The schema is registered on first use, not at construction.**
`AvroCodec.registered(...)` builds a codec; the `register` call happens the first
time it encodes something, once, and the identifier is remembered. That is why
the old service publishes first in this example — it is what makes its schema
version 1.

## Registered mode is a different message

```ruby
AvroCodec.registered(registry, subject: SUBJECT, schema: V1)   # application/vnd.acemq.avro
AvroCodec.of(V1)                                               # avro/binary
```

The two are not interchangeable. The five bytes of framing are invisible in the
body, so a fixed-schema codec handed a registered message would read the
identifier as the first field and hand back a record of silent nonsense. Each
mode therefore accepts only its own content type, and says so when refused.

## The registry here is not a registry

`InMemorySchemaRegistry` shares nothing between processes, so a consumer cannot
look up a schema a producer registered somewhere else — which is the entire point
of having one. It is here to show the shape. `Patterns::SQLSchemaRegistry`
outlives the process; Confluent's works too, because the wire framing is theirs.
A registry is anything answering `register`, `by_id`, `latest` and `versions`.

Which *header* would carry a schema identifier is deliberately not something
AceMQ has invented. The identifier lives in the body's framing, where Confluent
put it, so a Ruby producer and a Java consumer agree without either of them
having to know about the other.
