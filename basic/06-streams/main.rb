# frozen_string_literal: true

# Six readings written once and read three times, from three different places.
#
# A queue forgets a message the moment somebody acknowledges it. A stream does
# not: acknowledging moves *this consumer's* position and nothing else, so the
# messages are still there for the next reader, and the one after that. That is
# the whole difference, and everything else follows from it — the offset a
# consumer starts at, and the retention policy that is now the only thing
# deciding when a message goes away.

require "acemq/amqp"
require "acemq/amqp/patterns"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

STREAM = "rb-stream-sensor-readings"

READINGS = [
  { "sensor" => "roof-1", "celsius" => 19.0 },
  { "sensor" => "roof-1", "celsius" => 19.5 },
  { "sensor" => "roof-2", "celsius" => 21.0 },
  { "sensor" => "roof-2", "celsius" => 21.5 },
  { "sensor" => "roof-3", "celsius" => 17.0 },
  { "sensor" => "roof-3", "celsius" => 17.5 }
].freeze

# Waits for a reader to have seen what it was supposed to see, with a deadline
# rather than a sleep: a stream that stopped delivering should fail the example
# rather than pass it slowly.
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

mq = Connection.open(URL, origin: "examples/06-streams")

# `x-queue-type` is part of a queue's identity to the broker, so a name that
# already exists as something else cannot be re-declared as a stream — the
# answer is PRECONDITION_FAILED and it does not mention streams. This broker is
# shared with four other languages' examples.
mq.delete_queue(STREAM) if mq.queue_exists?(STREAM)

# Retention is not the optional-looking thing it looks like. A queue's messages
# leave when somebody handles them; a stream's leave when the policy says so,
# and a stream declared without one grows until the disk is full.
#
# `segment_bytes` is `x-stream-max-segment-size-bytes`, and it is here because
# retention discards a whole segment at a time: with RabbitMQ's 500 MB default,
# a stream whose `max_age` is an hour keeps everything until it has half a
# gigabyte to drop.
Patterns.declare_stream(mq, STREAM,
                        max_age: 3600, max_bytes: 20 * 1024 * 1024,
                        segment_bytes: 1024 * 1024)

READINGS.each do |reading|
  mq.publish(reading, to: STREAM, type: "reading.taken.v1")
end
puts "published #{READINGS.size} readings to #{STREAM}"

# ---------------------------------------------------------------------------
# A projection built from nothing, which is the reason to declare a stream at
# all: the history is still there to be read.
seen = Thread::Queue.new
projection = Patterns.read_stream(mq, STREAM, offset: Patterns::StreamOffset.first,
                                              name: "rb-projection") do |message|
  seen << message.payload["celsius"]
  Ack.accept
end
from_first = drain(seen, READINGS.size)
projection.cancel
puts "from_first:        #{from_first.inspect}"

# ---------------------------------------------------------------------------
# A consumer that wrote down where it got to and is carrying on. Offsets count
# from zero and are stable — message four is message four for every reader for
# ever — which is what makes writing one down worth anything.
resumed = Patterns.read_stream(mq, STREAM, offset: Patterns::StreamOffset.at(3),
                                           name: "rb-resumed") do |message|
  seen << message.payload["celsius"]
  Ack.accept
end
from_offset = drain(seen, 3)
resumed.cancel
puts "at(3):             #{from_offset.inspect}"

# ---------------------------------------------------------------------------
# And the ordinary case: everything already written is somebody else's problem.
tailing = Patterns.read_stream(mq, STREAM, offset: Patterns::StreamOffset.next,
                                           name: "rb-tail") do |message|
  seen << message.payload["celsius"]
  Ack.accept
end
# Long enough that a `next` consumer wrongly reading the history would have
# shown it before the new reading is published.
sleep 1
mq.publish({ "sensor" => "roof-4", "celsius" => 25.0 }, to: STREAM)
tail = drain(seen, 1)
tailing.cancel
puts "next:              #{tail.inspect}"

# ---------------------------------------------------------------------------
# And the proof that none of that removed anything. Three consumers have now
# acknowledged every message they read; a fourth starting at the beginning still
# finds all of them, the new one included.
#
# `message_count` is no use here and is worth knowing about: RabbitMQ answers a
# passive declare on a stream with zero however much it holds, because a stream
# has no single "how many are left" — it has a position per consumer. Reading it
# again is the only honest way to ask.
rereading = Patterns.read_stream(mq, STREAM, offset: Patterns::StreamOffset.first,
                                             name: "rb-reread") do |message|
  seen << message.payload["celsius"]
  Ack.accept
end
again = drain(seen, READINGS.size + 1)
rereading.cancel
puts "from_first again:  #{again.inspect}"

mq.delete_queue(STREAM)
mq.close

expected = READINGS.map { |reading| reading["celsius"] }
abort "from_first did not read the stream in order: #{from_first.inspect}" unless
  from_first == expected
abort "at(3) started somewhere else: #{from_offset.inspect}" unless
  from_offset == expected.drop(3)
abort "next did not start at the next message: #{tail.inspect}" unless tail == [25.0]

# The point of the whole thing. Three consumers acknowledged every message they
# read, and the fourth still finds all seven — acknowledging a stream message
# moves that consumer's position and removes nothing.
abort "the stream did not survive being read: #{again.inspect}" unless
  again == expected + [25.0]
