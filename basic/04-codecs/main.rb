# frozen_string_literal: true

# Four formats read off one queue by one consumer, which is what a format
# migration actually looks like: the new producer is deployed on Tuesday, the
# old one is still running on Wednesday, and the consumer has to read both.
#
# A `CompositeCodec` writes with the first codec it was given and reads with
# whichever of them answers for the message's content type. Nothing is guessed
# from the bytes — the content type is what decides, which is why only JSON and
# bytes will answer for a message that arrived without one.

require "acemq/amqp"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "catalogue-events"
QUEUE = "catalogue.updates"

# JSON first, so this connection writes JSON. The other three are here to be
# read. XMLCodec needs a root element name because XML has no anonymous
# document element and a Hash does not carry one.
CODEC = CompositeCodec.new(JSONCodec.new, YAMLCodec.new, TOMLCodec.new,
                           XMLCodec.new(root: "product"))

mq = Connection.open(URL, origin: "examples/04-codecs", codec: CODEC)

[QUEUE, Naming.dead_letter_queue(QUEUE)].each do |queue|
  mq.delete_queue(queue) if mq.queue_exists?(queue)
end

Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(QUEUE, dead_letter: true)
        .binding(QUEUE, EXCHANGE, "product.#")
        .apply(mq)

read = Thread::Queue.new

consumer = mq.consume(QUEUE) do |message|
  # `content_type` is on the message as well as the payload, which is what
  # anybody debugging a decoding disagreement between two languages needs: the
  # bytes are in `body` and the type that chose the codec is right beside them.
  read << [message.content_type, message.payload]
  Ack.accept
end

product = { "sku" => "X-1", "name" => "Ratchet", "price" => 1299 }

# One publish per format. `codec:` overrides the connection's for this message
# only, which is how a service migrating a format publishes the new one to a
# queue that still has the old on it.
[
  JSONCodec.new,
  YAMLCodec.new,
  TOMLCodec.new,
  XMLCodec.new(root: "product")
].each do |codec|
  mq.publish(product, to: "product.updated", exchange: EXCHANGE,
                      type: "product.updated.v1", codec: codec)
end

seen = []
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
while seen.size < 4 && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
  begin
    seen << read.pop(true)
  rescue ThreadError
    sleep 0.05
  end
end

consumer.cancel

seen.sort_by(&:first).each do |content_type, payload|
  puts format("%-20s %s", content_type, payload.inspect)
end

puts
puts "registered codecs: #{Codecs.names.inspect}"

mq.close

abort "expected 4 messages, got #{seen.size}" unless seen.size == 4

# Every one of them should have come back as the same product — but look at
# `price` in the output above. JSON, YAML and TOML all give back the integer
# 1299; XML gives back the string "1299", because XML has only text and nothing
# in the document says which of the two it was. That is a property of the
# format rather than of this library, and it is the sort of thing better found
# in an example than in a consumer that started rounding.
skus = seen.map { |_type, payload| payload["sku"] }
abort "a format did not round-trip: #{skus.inspect}" unless skus.uniq == ["X-1"]

types = seen.map(&:first).sort
expected = ["application/json", "application/toml", "application/xml", "application/yaml"]
abort "expected #{expected.inspect}, got #{types.inspect}" unless types == expected
