# frozen_string_literal: true

# A message delivered later, with no scheduler process, no plugin and no cron.
#
# The obvious way to do this is a per-message time to live, and it does not
# work: RabbitMQ expires messages only from the head of a queue, so a four-hour
# message put in front of a one-minute message delivers the one-minute message
# in four hours, with nothing reporting it.
#
# What happens instead is a ladder of queues each with a *uniform* time to live
# — acemq.schedule.{1h,10m,1m,10s,1s} — dead-lettering into acemq.schedule.due,
# where the scheduler either delivers the message or puts it in the largest
# rung that does not overshoot. Every message in a rung has the same delay, so
# head-of-line expiry is harmless. A one-day delay costs twenty-four hops; a
# one-minute delay costs one.

require "acemq/amqp"
require "acemq/amqp/patterns"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "reminders"
QUEUE = "reminders.due"

mq = Connection.open(URL, origin: "examples/07-scheduling")

[QUEUE, Naming.dead_letter_queue(QUEUE)].each do |queue|
  mq.delete_queue(queue) if mq.queue_exists?(queue)
end

Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(QUEUE, dead_letter: true)
        .binding(QUEUE, EXCHANGE, "reminder.#")
        .apply(mq)

arrived = Thread::Queue.new
consumer = mq.consume(QUEUE) do |message|
  arrived << [Process.clock_gettime(Process::CLOCK_MONOTONIC), message]
  Ack.accept
end

# Without a block this runs until `close`. The block form closes it on the way
# out, which is what a script that schedules and exits wants and not what this
# one does — here the scheduler has to still be running when the messages come
# due.
#
# `Scheduler.on` declares the exchange, the five rungs and the control queue.
# Every name, argument and header is shared with the Java, Go, .NET and Python
# libraries, because two services scheduling on one broker declare the same
# queues and a rung declared with different arguments answers the second one
# PRECONDITION_FAILED.
scheduler = Patterns::Scheduler.on(mq)
started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

# Anything in the past is delivered at once rather than refused: a renewal date
# that has already gone by is a reminder that is late, not an error.
scheduler.at(Time.now - 60, { "reminder_id" => "R-0", "asked_for" => "past" },
             to: "reminder.due", exchange: EXCHANGE)

scheduler.in(3, { "reminder_id" => "R-1", "asked_for" => 3 },
             to: "reminder.due", exchange: EXCHANGE)

scheduler.in(5, { "reminder_id" => "R-2", "asked_for" => 5 },
             to: "reminder.due", exchange: EXCHANGE)

seen = []
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 30
while seen.size < 3 && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
  begin
    seen << arrived.pop(true)
  rescue ThreadError
    sleep 0.05
  end
end

puts "scheduled #{scheduler.scheduled}, delivered #{scheduler.delivered}, " \
     "hops #{scheduler.hops}"
puts

# Look at the two columns. A message is delivered as soon as less than one
# second is left, because another hop through the smallest rung would cost more
# than the accuracy it buys — so a three-second delay lands at about two. That
# is the trade this design makes, stated rather than hidden: delivery is
# accurate to about the smallest rung, and something that has to fire at
# 09:00:00.000 wants a scheduler rather than a message broker.
puts "asked for   arrived at"
seen.sort_by { |at, _| at }.each do |at, message|
  asked = message.payload["asked_for"]
  puts format("%9s   %6.1fs   %s",
              asked.is_a?(Numeric) ? "#{asked}s" : asked, at - started,
              message.payload["reminder_id"])
end

# The payload is encoded once, when it is scheduled, and carried as bytes from
# then on with its content type in a header that is put back on the message
# finally delivered. A scheduler that decoded would acquire opinions about
# message formats it has no business having.
puts
puts "content type on arrival: #{seen.first&.last&.content_type}"

scheduler.close
consumer.cancel
mq.close

abort "expected 3 reminders, got #{seen.size}" unless seen.size == 3

by_id = seen.to_h { |at, message| [message.payload["reminder_id"], at - started] }

# The already-due one should not have waited for anything.
abort "the past-dated reminder waited #{by_id["R-0"]}s" unless by_id["R-0"] < 1.5

# And the delayed ones should have waited. This is the check worth having: a
# scheduler that delivered everything immediately would satisfy every other
# assertion here. The bound is loose by a second on purpose — see above.
abort "R-1 arrived too early: #{by_id["R-1"]}s" unless by_id["R-1"] >= 1.5
abort "R-2 arrived too early: #{by_id["R-2"]}s" unless by_id["R-2"] >= 3.5
abort "R-2 arrived before R-1" unless by_id["R-2"] > by_id["R-1"]
abort "nothing was delivered through the ladder" unless scheduler.delivered == 3
