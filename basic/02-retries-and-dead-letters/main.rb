# frozen_string_literal: true

# The attempt counter moving, a message running out of attempts, and a failure
# marked fatal skipping the wait entirely.
#
# The thing to watch is `x-acemq-attempt`. It travels on the message rather
# than being counted in the consumer, because a count kept in the consumer is
# wrong the moment a second consumer exists — a message moving between them
# would be on attempt one for ever — and a restart forgets it anyway.

require "acemq/amqp"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "payments-events"
QUEUE = "payments.charges"
DEAD_LETTERS = Naming.dead_letter_queue(QUEUE) # => "payments.charges.dlq"

# Three attempts, 200ms apart, doubling. Short on purpose: under the 30-second
# threshold the consumer waits out the delay itself, which is what makes this
# example finish in a second rather than needing a rung queue and a broker
# timer. `schedule` is the thing to read when deciding whether a policy is the
# one you meant — it shows the delays without jitter.
POLICY = RetryPolicy.exponential(3, 0.2, 5)

mq = Connection.open(URL, origin: "examples/02-retries", retry_policy: POLICY)

# This example counts deliveries, so it starts from an empty queue: a message
# left behind by an earlier run would be counted too, and the check at the
# bottom would fail for a reason that has nothing to do with retries.
[QUEUE, DEAD_LETTERS].each { |queue| mq.delete_queue(queue) if mq.queue_exists?(queue) }

# `retry_policy:` on the queue works out which rung queues the policy would
# need and declares them up front. None of them are used here — every delay is
# under the threshold — but declaring them is free, and a rung discovered one
# failure at a time is a rung that was missing when it mattered.
Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(QUEUE, dead_letter: true, retry_policy: POLICY)
        .binding(QUEUE, EXCHANGE, "charge.#")
        .apply(mq)

attempts = Thread::Queue.new

consumer = mq.consume(QUEUE, retry_policy: POLICY) do |message|
  attempts << message.envelope.attempt

  if message.payload["card"] == "stolen"
    # FatalError says "this will not work next time either". The remaining
    # attempts are abandoned and the message is dead-lettered at once — there
    # is nothing to be gained by asking a card that has been cancelled three
    # more times.
    raise FatalError, "the card was reported stolen"
  end

  # An ordinary failure. `Ack.retry` returns the message with the attempt
  # advanced; the reason travels with it and ends up in `x-acemq-error` if it
  # runs out of attempts.
  Ack.retry("the acquirer timed out")
end

mq.publish({ "charge_id" => "C-1", "card" => "good" },
           to: "charge.requested", exchange: EXCHANGE, type: "charge.requested.v1")
mq.publish({ "charge_id" => "C-2", "card" => "stolen" },
           to: "charge.requested", exchange: EXCHANGE, type: "charge.requested.v1")

# Four deliveries in total: three of C-1 (the policy's three attempts) and one
# of C-2, which gives up on the first.
seen = []
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 20
while seen.size < 4 && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
  begin
    seen << attempts.pop(true)
  rescue ThreadError
    sleep 0.05
  end
end

consumer.cancel

puts "deliveries   #{seen.sort.inspect}"
puts "schedule     #{POLICY.schedule.inspect}  (seconds, before jitter)"

# Both messages should now be on the dead-letter queue, with the reason written
# into the envelope. That reason is the entire value of dead-lettering rather
# than answering the broker with a bare `basic.reject`, which drops the message
# somewhere with nothing at all saying what happened to it. `Ack.reject` is a
# different thing and carries its reason too — it is how a handler says "this
# one is not processable", and it is the sentence on the first line below.
dead_letters = []
while (delivery = mq.pull(DEAD_LETTERS))
  envelope = Envelope.from_headers(delivery.headers, delivery.routing_key)
  dead_letters << envelope
  delivery.ack
end

dead_letters.sort_by { |e| e.headers.to_s }.each do |envelope|
  puts "dead letter  attempt #{envelope.attempt}: #{envelope.error}"
end

mq.close

# The counts are the claim, so they are checked rather than printed. Three
# attempts for the retried charge and one for the fatal one; anything else
# means the attempt counter has stopped moving, which is the failure this
# example exists to make visible.
abort "expected 4 deliveries, got #{seen.size}" unless seen.size == 4
unless seen.sort == [1, 1, 2, 3]
  abort "expected attempts [1, 1, 2, 3], got #{seen.sort.inspect}"
end
abort "expected 2 dead letters, got #{dead_letters.size}" unless dead_letters.size == 2
abort "a dead letter arrived without a reason" if dead_letters.any? { |e| e.error.empty? }
