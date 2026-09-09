# frozen_string_literal: true

# Two services on two versions of one schema, talking to each other anyway.
#
# The old service has not been redeployed and writes orders without a currency.
# The new one writes them with one. Neither knows what the other is running, and
# both read every message correctly — because each message carries the
# identifier of the schema it was *written* with, and the registry turns that
# identifier back into a schema the reader can resolve onto its own.
#
# `intermediate/05-binary-codecs` is about choosing a codec by content type and
# shows one corner of this — a reader ahead of its writer, filling a field in
# from a default. This is the registry itself, and the corner that costs money
# to get wrong: a *producer* ahead of its consumers.

require "acemq/amqp"
require "acemq/amqp/patterns"
require "acemq/amqp/codec/avro"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "rb-schema-orders"
OLD_QUEUE = "rb-schema-old-reader"
NEW_QUEUE = "rb-schema-new-reader"

# Groups the versions of one message type. Conventionally the type itself.
SUBJECT = "rb.order.placed"

V1 = <<~JSON
  { "type": "record", "name": "OrderPlaced", "namespace": "acemq.examples",
    "fields": [
      { "name": "order_id", "type": "string" },
      { "name": "total",    "type": "double" }
    ] }
JSON

# A field added, with a default. The default is not decoration: without one this
# is not a backwards-compatible change, and a v2 reader meeting a v1 message
# would have nothing to put in the field.
V2 = <<~JSON
  { "type": "record", "name": "OrderPlaced", "namespace": "acemq.examples",
    "fields": [
      { "name": "order_id", "type": "string" },
      { "name": "total",    "type": "double" },
      { "name": "currency", "type": "string", "default": "EUR" }
    ] }
JSON

# The identifier framed onto a registered message: one zero byte, four bytes of
# schema id, big-endian, then the Avro body. Confluent's framing, which Java, Go,
# .NET and Python all write.
def schema_id_on(body)
  body.to_s.b[1, 4].unpack1("N")
end

def drain(seen, how_many, within: 15)
  read = []
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + within
  while read.size < how_many && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
    begin
      read << seen.pop(true)
    rescue ThreadError
      sleep 0.05
    end
  end
  read
end

# Not a registry in the sense that matters: nothing is shared between processes,
# so a consumer cannot look up a schema a producer registered somewhere else,
# which is the entire point of having one. It is here to show the shape.
# `Patterns::SQLSchemaRegistry` outlives the process, and the wire framing is
# Confluent's, so theirs works too.
registry = Patterns::InMemorySchemaRegistry.new

# Two services, each holding the schema it was written against. Each writes with
# it and resolves every message it reads onto it. The schema is registered the
# first time the codec encodes something, not here — a producer that registers on
# every start would otherwise add a version per restart.
old_service = AvroCodec.registered(registry, subject: SUBJECT, schema: V1)
new_service = AvroCodec.registered(registry, subject: SUBJECT, schema: V2)

mq = Connection.open(URL, origin: "examples/10-schema-evolution")

Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(OLD_QUEUE, dead_letter: true)
        .binding(OLD_QUEUE, EXCHANGE, "order.placed")
        .queue(NEW_QUEUE, dead_letter: true)
        .binding(NEW_QUEUE, EXCHANGE, "order.placed")
        .apply(mq)

by_old = Thread::Queue.new
by_new = Thread::Queue.new

readers = [
  mq.consume(OLD_QUEUE, codec: old_service) { |m| by_old << m and Ack.accept },
  mq.consume(NEW_QUEUE, codec: new_service) { |m| by_new << m and Ack.accept }
]

# The old service publishing, having never heard of a currency. It goes first so
# that its schema is the one registered as version 1.
mq.publish({ "order_id" => "A-1", "total" => 42.0 },
           to: "order.placed", exchange: EXCHANGE, type: "order.placed.v1",
           codec: old_service)
# And the new one, which has been redeployed.
mq.publish({ "order_id" => "B-2", "total" => 99.5, "currency" => "GBP" },
           to: "order.placed", exchange: EXCHANGE, type: "order.placed.v2",
           codec: new_service)

old_read = drain(by_old, 2)
new_read = drain(by_new, 2)
readers.each(&:cancel)

versions = registry.versions(SUBJECT)
puts "versions of #{SUBJECT}: #{versions.map(&:to_s).inspect}"
puts

puts "reader   written with  decoded"
{ "v1" => old_read, "v2" => new_read }.each do |version, messages|
  messages.sort_by { |m| m.payload["order_id"] }.each do |message|
    puts format("%-8s %-13s %s", version, "id #{schema_id_on(message.body)}",
                message.payload.inspect)
  end
end

[OLD_QUEUE, NEW_QUEUE].each do |queue|
  [queue, Naming.dead_letter_queue(queue), Naming.parked_queue(queue)].each do |name|
    mq.delete_queue(name) if mq.queue_exists?(name)
  end
end
mq.close

# ---------------------------------------------------------------------------
# A reader built after the fact, which has been told about no schema but the one
# it holds. Ruby's registered codec looks an unknown identifier up when it meets
# it, so this reads a v2 message it has never seen before. Pointed at a registry
# that does not hold the schema, it says so rather than guessing.
v2_bytes = new_service.encode({ "order_id" => "C-3", "total" => 1.0, "currency" => "USD" })
latecomer = AvroCodec.registered(registry, subject: SUBJECT, schema: V1)
late_read = latecomer.decode(v2_bytes, AvroCodec::REGISTERED_CONTENT_TYPE)

stranger = AvroCodec.registered(Patterns::InMemorySchemaRegistry.new,
                                subject: SUBJECT, schema: V1)
refusal = begin
  stranger.decode(v2_bytes, AvroCodec::REGISTERED_CONTENT_TYPE)
  nil
rescue Patterns::SchemaNotFound, DecodeError => e
  e.message
end
puts
puts "a reader with the wrong registry: #{refusal}"

# ---------------------------------------------------------------------------
abort "the subject does not have two versions: #{versions.map(&:to_s)}" unless
  versions.map(&:version) == [1, 2]
abort "version 2 is not the one with a currency in it" unless
  versions.last.definition.include?("currency")

# Registering the same definition again is not a new version. Without that, a
# service that registers on every start adds one per restart and the version
# number stops meaning anything.
again = registry.register(SUBJECT, "avro", versions.last.definition)
abort "registering v2 twice made a second version: #{again.id} and #{versions.last.id}" unless
  again.id == versions.last.id
abort "the fingerprint is not a hash of the definition" unless
  again.fingerprint == Patterns.fingerprint(versions.last.definition)
abort "by_id does not find what register returned" unless
  registry.by_id(versions.last.id).definition == versions.last.definition

old_by_order = old_read.to_h { |m| [m.payload["order_id"], m] }
new_by_order = new_read.to_h { |m| [m.payload["order_id"], m] }
abort "the v1 reader did not see both orders: #{old_by_order.keys}" unless
  old_by_order.keys.sort == %w[A-1 B-2]
abort "the v2 reader did not see both orders: #{new_by_order.keys}" unless
  new_by_order.keys.sort == %w[A-1 B-2]

# Every message is framed with the schema it was *written* with, whatever the
# reader holds. That identifier is the only thing on the wire that makes any of
# the rest work.
abort "the v2 message is not framed with the v2 schema id" unless
  schema_id_on(old_by_order["B-2"].body) == versions.last.id
abort "the v1 message is not framed with the v1 schema id" unless
  schema_id_on(new_by_order["A-1"].body) == versions.first.id
abort "a registered message did not say so: #{old_by_order["B-2"].content_type}" unless
  old_by_order["B-2"].content_type.start_with?(AvroCodec::REGISTERED_CONTENT_TYPE)

# The direction that costs money to get wrong: a producer already on the new
# schema, a consumer still on the old one. The field it does not know is skipped,
# and the fields it does know are the right way round.
old_reading_new = old_by_order["B-2"].payload
abort "the v1 reader invented a currency: #{old_reading_new.inspect}" if
  old_reading_new.key?("currency")
abort "the v1 reader misread a v2 message: #{old_reading_new.inspect}" unless
  old_reading_new == { "order_id" => "B-2", "total" => 99.5 }

# And the other way, which is what the default in V2 is for.
new_reading_old = new_by_order["A-1"].payload
abort "the reader's default was not applied: #{new_reading_old.inspect}" unless
  new_reading_old == { "order_id" => "A-1", "total" => 42.0, "currency" => "EUR" }
abort "the v2 reader lost the currency the v2 writer wrote" unless
  new_by_order["B-2"].payload["currency"] == "GBP"

abort "a codec that had never met id #{versions.last.id} did not ask the registry" unless
  late_read == { "order_id" => "C-3", "total" => 1.0 }
abort "a reader with the wrong registry decoded the message anyway" if refusal.nil?
