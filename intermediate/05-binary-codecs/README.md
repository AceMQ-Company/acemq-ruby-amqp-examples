# intermediate/05 — Protobuf, Avro and a schema registry

Two binary messages on one queue, and an Avro schema that changed underneath
the consumer without breaking it.

## What it shows

- **`ProtobufCodec` reads and writes one message type**, and the generated
  class is the schema.
- **`AvroCodec.registered` puts a schema identifier in front of the bytes**, so
  the writer's schema can be resolved onto the reader's.
- **Neither codec answers for a message with no content type.** Protobuf and
  Avro bytes are not recognisable and parse quietly into nonsense more often
  than they fail, so a codec that volunteered would report that nonsense as a
  success.

## Running it

```bash
docker compose up -d --wait
bundle exec ruby intermediate/05-binary-codecs/main.rb
```

## What to look for

```
application/vnd.acemq.avro     20 bytes  {"sensor" => "roof-2", "celsius" => 19.25, "unit" => "C"}
application/x-protobuf         17 bytes  <Reading: sensor: "roof-1", celsius: 21.5>

registered schema versions: 1
```

**`"unit" => "C"` was never written.** The producer's schema does not have that
field; the consumer's does, with a default. The writer's schema travelled as an
identifier, the registry resolved it, and the field was filled in — which is
the whole of what schema evolution needs and the only reason a registry is
worth running. A field the reader does not know is skipped the same way —
[`intermediate/10-schema-evolution`](../10-schema-evolution) is that half, which
is the direction that reaches production first: a producer redeployed onto a new
schema while its consumers are still on the old one.

**The protobuf message comes back as `Reading`, not as a Hash.** The class is
the schema, so there is nothing to convert it to. Publishing anything else
through that codec is an `EncodeError` naming both types.

Both messages are under twenty bytes. The same product as JSON is about sixty.

## Two content types for Avro

`avro/binary` when the codec has a fixed schema, `application/vnd.acemq.avro`
when it has a registry. They are different content types because they are
different messages: one has a schema identifier in front of it and one does
not, and a consumer that guessed would read the identifier as data.

## The descriptor built at run time

In a real project the protobuf type is a `reading_pb.rb` that `protoc` wrote,
and `require`ing it is all you do. It is built here from `descriptor_pb`
instead, so that running this example needs no protoc and no build step. The
descriptor is exactly what protoc would have produced and `ProtobufCodec`
cannot tell the difference, because there is none to tell.

## The registry here is the wrong one

`InMemorySchemaRegistry` shares nothing between processes, which is the entire
point of a registry. It is for tests and for seeing the shape of the thing;
`Patterns::SQLSchemaRegistry` is the one that outlives the process.

Registering the same definition twice returns the same identifier rather than
making a second version — otherwise a service that registers on every start
adds a version per restart. The fingerprint is SHA-256 of the exact bytes, so
two definitions differing only in whitespace count as different: normalising
would need a parser per format, and a registry that quietly treated two
definitions as one because it mis-parsed them would be worse than a strict one.

## Gemfile

`google-protobuf` and `avro` are required lazily by the library and named in
the error when absent, which is why they are in this repository's Gemfile and
not in the gem's dependencies. A service publishing JSON should not be made to
compile a protobuf runtime to do it.
