# frozen_string_literal: true

# Four slow invoices, handled four times faster by four consumers than by one.
#
# `concurrency: 4` and a group of four look like the same thing and are not. One
# consumer is one channel with one prefetch, however many handlers run behind
# it, so four handlers still take their messages one at a time. Four consumers
# are four channels with four prefetches, and the broker round-robins between
# them.
#
# The example runs the same four messages both ways and prints how long each
# took.

require "acemq/amqp"
require "acemq/amqp/patterns"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

QUEUE = "rb-group-invoices"

INVOICES = %w[INV-1 INV-2 INV-3 INV-4].freeze

# Long enough that two handlers overlapping is not a coincidence, and short
# enough that the slow half of this is still under three seconds.
WORK = 0.5

# Counts handlers running at the same moment, which is the number the whole
# example is about. Handlers run on the transport's threads, so the counter
# needs a lock; everything else — elapsed time, invoices handled — follows.
class Watcher
  attr_reader :most, :handled

  def initialize
    @lock = Mutex.new
    @running = 0
    @most = 0
    @handled = []
  end

  def handle(message)
    @lock.synchronize do
      @running += 1
      @most = [@most, @running].max
    end
    sleep WORK
    @lock.synchronize do
      @handled << message.payload["invoice"]
      @running -= 1
    end
    Ack.accept
  end

  def done? = @lock.synchronize { @handled.size == INVOICES.size }
end

def fill(connection)
  INVOICES.each { |invoice| connection.publish({ "invoice" => invoice }, to: QUEUE) }
end

def time_until_done(watcher, what, within: 30)
  started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  deadline = started + within
  sleep 0.05 until watcher.done? || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
  abort "#{what} handled #{watcher.handled.size} of #{INVOICES.size} invoices" unless
    watcher.done?

  Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
end

mq = Connection.open(URL, origin: "examples/09-consumer-groups")

Topology.new.queue(QUEUE, dead_letter: true).apply(mq)

# ---------------------------------------------------------------------------
# Four consumers, each with its own channel and its own prefetch of one.
grouped = Watcher.new
fill(mq)

group = Patterns::ConsumerGroup.new(mq, QUEUE, size: 4, prefetch: 1) do |message|
  grouped.handle(message)
end
puts "group of #{group.size} on #{group.queue}"
started_consumers = group.consumers
group_took = time_until_done(grouped, "the group")

# One call, and every one of them is stopped — including the handlers already
# running, which are waited for. Four consumers started by hand are four things
# to remember to stop, and a partial shutdown leaves messages held by a consumer
# nobody is waiting for.
group.close
puts "still running after close: #{started_consumers.count(&:running?)}"

# ---------------------------------------------------------------------------
# One consumer running four handlers, over the same four messages.
alone = Watcher.new
fill(mq)

one = mq.consume(QUEUE, prefetch: 1, concurrency: 4, tag: "rb-invoices-1") do |message|
  alone.handle(message)
end
single_took = time_until_done(alone, "the single consumer")
one.cancel

puts
puts "#{" " * 28} #{"at once".rjust(8)} #{"seconds".rjust(8)}"
puts format("%-28s %8d %8.1f", "group of 4, prefetch 1", grouped.most, group_took)
puts format("%-28s %8d %8.1f", "1 consumer, concurrency 4", alone.most, single_took)

[QUEUE, Naming.dead_letter_queue(QUEUE), Naming.parked_queue(QUEUE)].each do |name|
  mq.delete_queue(name) if mq.queue_exists?(name)
end
mq.close

abort "the group lost or duplicated an invoice: #{grouped.handled.inspect}" unless
  grouped.handled.sort == INVOICES
abort "the single consumer lost an invoice: #{alone.handled.inspect}" unless
  alone.handled.sort == INVOICES

# The claim the pattern makes: four consumers hold four messages at once because
# there are four prefetches, and one consumer holds one however many handlers
# are behind it.
abort "a group of four held #{grouped.most} messages at once, not four" unless
  grouped.most == INVOICES.size
unless alone.most == 1
  abort "one consumer with a prefetch of one held #{alone.most} messages at once; " \
        "the prefetch is per consumer and this is no longer true"
end
unless group_took < single_took
  abort "the group took #{group_took.round(1)}s and one consumer " \
        "#{single_took.round(1)}s, which is not what four prefetches buy"
end
abort "closing the group left a consumer running" unless started_consumers.none?(&:running?)
