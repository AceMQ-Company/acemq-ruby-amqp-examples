# frozen_string_literal: true

# Dead letters put back once the fix is out — some of them, deliberately, and
# with the count of what moved and what did not.
#
# This is the thing somebody actually does at three in the morning: a
# dead-letter queue has a few hundred messages on it, one tenant's integration
# was the cause, and they need to go back through — but not all of them at
# once, and not silently.

require "acemq/amqp"
require "acemq/amqp/patterns"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "invoices-events"
QUEUE = "invoices.issued"
DEAD_LETTERS = Naming.dead_letter_queue(QUEUE)

# One attempt, so a failure dead-letters immediately. This example is about
# what happens afterwards.
mq = Connection.open(URL, origin: "examples/05-replay", retry_policy: RetryPolicy.none)

[QUEUE, DEAD_LETTERS].each { |queue| mq.delete_queue(queue) if mq.queue_exists?(queue) }

Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(QUEUE, dead_letter: true)
        .binding(QUEUE, EXCHANGE, "invoice.#")
        .apply(mq)

# ---------------------------------------------------------------------------
# The outage: six invoices, and the currency converter is down for two tenants.

broken = mq.consume(QUEUE) do |message|
  if message.payload["tenant"] == "acme"
    Ack.retry("the currency service timed out")
  else
    Ack.accept
  end
end

6.times do |n|
  mq.publish({ "invoice_id" => "I-#{n + 1}", "tenant" => n.even? ? "acme" : "globex" },
             to: "invoice.issued", exchange: EXCHANGE, type: "invoice.issued.v1")
end

deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
sleep 0.1 while mq.message_count(DEAD_LETTERS) < 3 &&
                Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline

broken.cancel
puts "dead letters after the outage: #{mq.message_count(DEAD_LETTERS)}"

# ---------------------------------------------------------------------------
# The fix is deployed, and this consumer accepts everything.

replayed = Thread::Queue.new
fixed = mq.consume(QUEUE) do |message|
  replayed << message
  Ack.accept
end

# Only the ones the converter broke, and only three at a time. The block is
# what makes a replay something that can be done in stages: everything it
# declines stays where it is.
#
# `routing_key:` is given rather than left alone here so the messages go back
# through the topic exchange onto `invoices.issued`. Left alone, a dead
# letter's routing key is the dead-letter queue it is sitting on — which is why
# a replay through the default exchange with no routing key is refused outright
# rather than allowed to move a queue onto itself for ever.
result = Patterns.replay(mq, from: DEAD_LETTERS, exchange: EXCHANGE,
                             routing_key: "invoice.issued", limit: 2) do |envelope, _body|
  envelope.error.include?("currency")
end

puts "first pass:  #{result}"

# The rest of them, with no limit.
rest = Patterns.replay(mq, from: DEAD_LETTERS, exchange: EXCHANGE,
                           routing_key: "invoice.issued")
puts "second pass: #{rest}"

seen = []
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
while seen.size < 3 && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
  begin
    seen << replayed.pop(true)
  rescue ThreadError
    sleep 0.05
  end
end

fixed.cancel

seen.sort_by { |m| m.payload["invoice_id"] }.each do |message|
  puts format("  %s  attempt %d  replayed from %s",
              message.payload["invoice_id"],
              message.envelope.attempt,
              message.envelope.headers[Patterns::REPLAYED_FROM_HEADER])
end

mq.close

abort "expected the first pass to move 2, got #{result.moved}" unless result.moved == 2
abort "expected the second pass to move 1, got #{rest.moved}" unless rest.moved == 1
abort "expected 3 messages back on #{QUEUE}, got #{seen.size}" unless seen.size == 3

# A replayed message goes back on attempt one with `x-acemq-error` cleared.
# Anything else does nothing that can be seen from outside: a message
# dead-lettered on its last attempt would arrive back on that attempt, be given
# up on before the handler ran, and move from the dead-letter queue to the
# dead-letter queue.
attempts = seen.map { |m| m.envelope.attempt }.uniq
abort "a replayed message did not restart: #{attempts.inspect}" unless attempts == [1]
abort "a replayed message kept its error" unless seen.all? { |m| m.envelope.error.empty? }
