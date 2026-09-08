# frozen_string_literal: true

# A durable queue, a published message, and a consumer that says what it read.
#
# The smallest thing worth running. Everything else in this repository is this
# with one more idea added.

require "acemq/amqp"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "orders-events"
QUEUE = "orders.new"

# `origin` is stamped on every message this connection publishes and travels in
# `x-acemq-origin`. It costs nothing and it is the difference between a
# dead-lettered message you can trace to a pod and one you cannot.
mq = Connection.open(URL, origin: "examples/01-publish-and-consume")

# A topic exchange, a durable quorum queue, and a binding between them. The
# queue asks for dead-lettering, which declares `orders.new.dlq` beside it —
# nothing here will use it, but a queue with nowhere to put a message it cannot
# handle is a queue that drops one.
Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(QUEUE, dead_letter: true)
        .binding(QUEUE, EXCHANGE, "order.#")
        .apply(mq)

# Handlers run on the transport's threads, not this one, so what they see has
# to cross back. A Thread::Queue is the plainest way to do that.
seen = Thread::Queue.new

consumer = mq.consume(QUEUE) do |message|
  seen << message
  # Every handler must return an Ack. `accept` is "done with it, remove it".
  Ack.accept
end

mq.publish({ "order_id" => "A-1", "total" => 4299 },
           to: "order.placed",
           exchange: EXCHANGE,
           type: "order.placed.v2")

# Nothing here has a blocking read with a deadline that works on Ruby 3.1 —
# Thread::Queue#pop only grew a timeout in 3.2 — so this polls, which is also
# the shape that lets an example fail with something readable rather than hang
# until CI kills it.
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
message = nil
until message || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
  begin
    message = seen.pop(true)
  rescue ThreadError
    sleep 0.05
  end
end

consumer.cancel
mq.close

abort "nothing arrived on #{QUEUE} within 15 seconds" unless message

puts "payload      #{message.payload.inspect}"
puts "type         #{message.envelope.type}"
puts "id           #{message.envelope.id}"
# Defaults to the id, so the next message in the chain has something to copy
# rather than a decision to make.
puts "correlation  #{message.envelope.correlation_id}"
puts "origin       #{message.envelope.origin}"
puts "attempt      #{message.envelope.attempt}"
puts "routing key  #{message.routing_key}"

# The envelope is the wire contract, so it is worth checking rather than
# printing: an example that prints whatever it got would go on passing after
# the field it means to demonstrate stopped being set.
abort "the type did not survive the trip" unless message.envelope.type == "order.placed.v2"
abort "the payload did not survive the trip" unless message.payload["order_id"] == "A-1"
abort "a first delivery should be attempt 1" unless message.envelope.attempt == 1
