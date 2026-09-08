# frozen_string_literal: true

# Three steps that change the world, a fourth that fails, and the first three
# undone in the order the world was changed in.
#
# A saga is not a distributed transaction. After `take-payment` the money has
# really moved, and the refund is a new fact rather than an erasure of the old
# one — which is why the result reports what was compensated rather than
# pretending nothing happened. And it is not durable: a crash midway leaves it
# half-applied with nothing to resume it.
#
# Nothing in `Patterns::Saga` touches a broker. It is here on a consumer
# because that is where one usually runs: a booking arrives as a message, the
# steps are calls to other systems, and the outcome is published as another
# message.

require "acemq/amqp"
require "acemq/amqp/patterns"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

BOOKINGS = "travel.bookings"
OUTCOMES = "travel.outcomes"

mq = Connection.open(URL, origin: "examples/06-saga")

[BOOKINGS, OUTCOMES, Naming.dead_letter_queue(BOOKINGS)].each do |queue|
  mq.delete_queue(queue) if mq.queue_exists?(queue)
end

Topology.new
        .queue(BOOKINGS, dead_letter: true)
        .queue(OUTCOMES)
        .apply(mq)

# The world, such as it is. Every step appends what it did so the order things
# happened in — and the order they were undone in — is visible at the end.
world = []
world_lock = Mutex.new
note = ->(what) { world_lock.synchronize { world << what } }

booking = Patterns::Saga.named("place-booking") do |saga|
  saga.step("take-payment") { |order| note.call("charged #{order["total"]}") }
      .compensate_with { |order| note.call("refunded #{order["total"]}") }

  saga.step("reserve-seat") { |order| note.call("reserved seat on #{order["flight"]}") }
      .compensate_with { |order| note.call("released seat on #{order["flight"]}") }

  # No compensation. A step that only read something needs no undoing, and a
  # saga that insisted on one for every step would collect empty blocks.
  saga.step("check-passport") { |_order| note.call("passport checked") }

  saga.step("book-hotel") do |order|
    raise "no rooms in #{order["city"]}" if order["city"] == "Reykjavik"

    note.call("booked a hotel in #{order["city"]}")
  end
end

puts "steps: #{booking.step_names.inspect}"
puts

consumer = mq.consume(BOOKINGS) do |message|
  # `run` returns a result rather than raising. A failed saga is not an
  # exceptional condition to a caller that has to decide what happens next —
  # and here what happens next is a message.
  result = booking.run(message.payload)

  mq.publish({ "booking_id" => message.payload["booking_id"],
               "outcome" => result.complete? ? "confirmed" : "compensated",
               "failed_at" => result.failed_at,
               "unresolved" => result.unresolved },
             to: OUTCOMES, type: "booking.settled.v1",
             causation_id: message.envelope.id)
  Ack.accept
end

collected = Thread::Queue.new
watcher = mq.consume(OUTCOMES) do |message|
  collected << message.payload
  Ack.accept
end

mq.publish({ "booking_id" => "B-1", "total" => 480, "flight" => "BA117", "city" => "Lisbon" },
           to: BOOKINGS, type: "booking.requested.v1")
mq.publish({ "booking_id" => "B-2", "total" => 990, "flight" => "FI451",
             "city" => "Reykjavik" },
           to: BOOKINGS, type: "booking.requested.v1")

settled = []
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 20
while settled.size < 2 && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
  begin
    settled << collected.pop(true)
  rescue ThreadError
    sleep 0.05
  end
end

consumer.cancel
watcher.cancel

settled.sort_by { |s| s["booking_id"] }.each do |outcome|
  puts format("%s  %-12s failed_at=%s unresolved=%s",
              outcome["booking_id"], outcome["outcome"],
              outcome["failed_at"].inspect, outcome["unresolved"].inspect)
end

puts
puts "what happened, in order:"
world_lock.synchronize { world.each { |what| puts "  #{what}" } }

mq.close

abort "expected 2 outcomes, got #{settled.size}" unless settled.size == 2

by_id = settled.to_h { |outcome| [outcome["booking_id"], outcome] }
abort "B-1 should have completed" unless by_id["B-1"]["outcome"] == "confirmed"
abort "B-2 should have compensated" unless by_id["B-2"]["outcome"] == "compensated"
abort "B-2 failed at the wrong step" unless by_id["B-2"]["failed_at"] == "book-hotel"

# Nothing was left for a person to reconcile. When a compensation itself fails
# the remaining ones still run — stopping would leave more undone than carrying
# on — and the step goes into `unresolved`, which is the set of real-world
# effects somebody now has to sort out by hand. That is the field to alert on.
abort "something was left unresolved" unless by_id["B-2"]["unresolved"].empty?

# Compensation runs in reverse, because that is the order the world was changed
# in: the seat is released before the payment is refunded.
undone = world.select { |what| what.start_with?("released", "refunded") }
abort "compensation ran in the wrong order: #{undone.inspect}" unless
  undone == ["released seat on FI451", "refunded 990"]
