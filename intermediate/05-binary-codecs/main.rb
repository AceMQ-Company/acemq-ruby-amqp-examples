# frozen_string_literal: true

# Protobuf and Avro on one queue, and an Avro schema that changed underneath a
# consumer without breaking it.
#
# These are the formats where the schema is not in the message. Protobuf's
# lives in a generated class; Avro's lives either in the consumer or, with a
# registry, behind an identifier carried in front of the bytes. Neither is
# recognisable from the bytes alone, which is why neither codec will answer for
# a message that arrived with no content type at all: a codec that volunteered
# there would decode whatever turned up and report nonsense as a success.

require "acemq/amqp"
require "acemq/amqp/patterns"
require "google/protobuf"
require "google/protobuf/descriptor_pb"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "telemetry-events"
QUEUE = "telemetry.readings"

# ---------------------------------------------------------------------------
# The protobuf type.
#
# In a real project this is a `reading_pb.rb` that `protoc` wrote from a
# `.proto` file, and `require`ing it is the whole of what you do. It is built
# here instead so that running this example needs no protoc and no build step —
# the descriptor below is exactly what protoc would have produced, and
# `ProtobufCodec` cannot tell the difference because there is none to tell.
Google::Protobuf::DescriptorPool.generated_pool.add_serialized_file(
  Google::Protobuf::FileDescriptorProto.new(
    name: "reading.proto", package: "acemq.examples", syntax: "proto3",
    message_type: [
      Google::Protobuf::DescriptorProto.new(name: "Reading", field: [
                                              Google::Protobuf::FieldDescriptorProto.new(
                                                name: "sensor", number: 1,
                                                type: :TYPE_STRING, label: :LABEL_OPTIONAL
                                              ),
                                              Google::Protobuf::FieldDescriptorProto.new(
                                                name: "celsius", number: 2,
                                                type: :TYPE_DOUBLE, label: :LABEL_OPTIONAL
                                              )
                                            ])
    ]
  ).to_proto
)
Reading = Google::Protobuf::DescriptorPool.generated_pool
                                          .lookup("acemq.examples.Reading").msgclass

# ---------------------------------------------------------------------------
# The Avro schemas. The reader's has a field the writer's does not, with a
# default — which is the whole of what schema evolution needs, and the reason a
# registry exists: the writer's schema is resolved onto the reader's, so a
# field the reader does not know is skipped and one the writer omitted is
# filled in.
WRITER_SCHEMA = <<~JSON
  { "type": "record", "name": "Reading", "namespace": "acemq.examples",
    "fields": [
      { "name": "sensor",  "type": "string" },
      { "name": "celsius", "type": "double" }
    ] }
JSON

READER_SCHEMA = <<~JSON
  { "type": "record", "name": "Reading", "namespace": "acemq.examples",
    "fields": [
      { "name": "sensor",  "type": "string" },
      { "name": "celsius", "type": "double" },
      { "name": "unit",    "type": "string", "default": "C" }
    ] }
JSON

# A registry is shared state, so an in-memory one is for tests and for seeing
# the shape of the thing — nothing is shared between processes, which is the
# entire point of a registry. `Patterns::SQLSchemaRegistry` is the one that
# outlives the process.
registry = Patterns::InMemorySchemaRegistry.new

protobuf = ProtobufCodec.new(Reading)
avro_writer = AvroCodec.registered(registry, subject: "telemetry.reading",
                                             schema: WRITER_SCHEMA)
avro_reader = AvroCodec.registered(registry, subject: "telemetry.reading",
                                             schema: READER_SCHEMA)

# The consumer reads either. `CompositeCodec` picks by content type:
# `application/x-protobuf` for one, `application/vnd.acemq.avro` for the other
# — the registered framing has a content type of its own precisely because a
# message with a schema identifier in front of it is a different message from
# one without.
mq = Connection.open(URL, origin: "examples/05-binary-codecs",
                          codec: CompositeCodec.new(protobuf, avro_reader))

[QUEUE, Naming.dead_letter_queue(QUEUE)].each do |queue|
  mq.delete_queue(queue) if mq.queue_exists?(queue)
end

Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(QUEUE, dead_letter: true)
        .binding(QUEUE, EXCHANGE, "reading.#")
        .apply(mq)

read = Thread::Queue.new
consumer = mq.consume(QUEUE) do |message|
  read << [message.content_type, message.body.bytesize, message.payload]
  Ack.accept
end

mq.publish(Reading.new(sensor: "roof-1", celsius: 21.5),
           to: "reading.taken", exchange: EXCHANGE, type: "reading.taken.v1",
           codec: protobuf)

# Written against the old schema, by a producer that has not been redeployed.
mq.publish({ "sensor" => "roof-2", "celsius" => 19.25 },
           to: "reading.taken", exchange: EXCHANGE, type: "reading.taken.v1",
           codec: avro_writer)

seen = []
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
while seen.size < 2 && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
  begin
    seen << read.pop(true)
  rescue ThreadError
    sleep 0.05
  end
end

consumer.cancel

seen.sort_by(&:first).each do |content_type, bytes, payload|
  puts format("%-30s %2d bytes  %s", content_type, bytes, payload.inspect)
end

puts
puts "registered schema versions: #{registry.latest("telemetry.reading").version}"

mq.close

abort "expected 2 messages, got #{seen.size}" unless seen.size == 2

protobuf_message = seen.find { |type, _, _| type.start_with?("application/x-protobuf") }
avro_message = seen.find { |type, _, _| type.include?("avro") }
abort "the protobuf message did not arrive" unless protobuf_message
abort "the avro message did not arrive" unless avro_message

# The protobuf message comes back as the generated type rather than as a Hash:
# the class is the schema, so there is nothing to convert it to.
decoded = protobuf_message.last
abort "protobuf did not round-trip: #{decoded.inspect}" unless decoded.is_a?(Reading)
abort "protobuf lost the sensor" unless decoded.sensor == "roof-1"

# And the Avro message arrives with the field the writer never wrote, filled in
# from the reader's default. Nothing about the producer changed.
avro = avro_message.last
abort "avro lost the sensor" unless avro["sensor"] == "roof-2"
abort "the reader's default was not applied: #{avro.inspect}" unless avro["unit"] == "C"
