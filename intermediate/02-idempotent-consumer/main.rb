# frozen_string_literal: true

# One logical message delivered four times and charged once.
#
# Delivery is at-least-once, and it is at-least-once whatever anybody does: the
# handler can finish and the acknowledgement can be lost on its way back to the
# broker, at which point the message is redelivered and the work has already
# happened. The only place that gap can actually be closed is a store written
# in the same transaction as the work — which is why this is a guard against
# duplicates rather than exactly-once, and why the store below says so about
# itself.

require "acemq/amqp"
require "acemq/amqp/patterns"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "billing-events"
QUEUE = "billing.charges"

mq = Connection.open(URL, origin: "examples/02-idempotent-consumer")

[QUEUE, Naming.dead_letter_queue(QUEUE)].each do |queue|
  mq.delete_queue(queue) if mq.queue_exists?(queue)
end

Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(QUEUE, dead_letter: true)
        .binding(QUEUE, EXCHANGE, "charge.#")
        .apply(mq)

# Right behind one worker and wrong the moment there are two: each process has
# its own memory, so both are told they are first. The store worth having is
# your own database, written in the same transaction as the charge.
store = Patterns::InMemoryIdempotencyStore.new(window: 3600)

charged = []
delivered = Thread::Queue.new
lock = Mutex.new

# `idempotent` wraps a handler and hands back a handler, so it goes to
# `consume` unchanged: the retry policy, the dead-lettering and the envelope
# are all still whatever the connection was configured with. A pattern that
# took over the consumer would have to reimplement them, and then there would
# be two retry engines to keep in step.
handler = Patterns.idempotent(store) do |message|
  lock.synchronize { charged << message.payload["charge_id"] }
  Ack.accept
end

consumer = mq.consume(QUEUE) do |message|
  ack = handler.call(message)
  delivered << message.envelope.id
  ack
end

# The same message, four times. Same id, so the same idempotency key: the id
# is the default key, which is the right default because it is what a broker
# redelivers unchanged.
envelope = Envelope.new(type: "charge.requested.v1", origin: mq.origin)
4.times do
  mq.publish({ "charge_id" => "C-9", "amount" => 4299 },
             to: "charge.requested", exchange: EXCHANGE, envelope: envelope)
end

seen = 0
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
while seen < 4 && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
  begin
    delivered.pop(true)
    seen += 1
  rescue ThreadError
    sleep 0.05
  end
end

consumer.cancel

puts "delivered  #{seen}"
puts "charged    #{charged.size} (#{charged.inspect})"

# A duplicate is accepted, not rejected. The work was done, so the message has
# been handled — dead-lettering it would raise an alarm about something that
# went right, and somebody would spend a morning on a queue full of successes.
dlq = Naming.dead_letter_queue(QUEUE)
puts
puts "the three duplicates were accepted rather than dead-lettered:"
puts "  #{dlq} holds #{mq.message_count(dlq)}"

mq.close

abort "expected 4 deliveries, got #{seen}" unless seen == 4
abort "expected exactly one charge, got #{charged.size}" unless charged.size == 1
