# frozen_string_literal: true

# A shutdown that waits for the handler in hand, and gives up at its deadline.
#
# Kubernetes sends SIGTERM and starts a clock. When it runs out the process is
# killed, and whatever is still inside a handler dies with it. `Connection#close`
# is the part of that a service controls: it stops every consumer, waits for the
# handlers already running, and only then shuts the socket — within one deadline,
# twenty seconds unless told otherwise.
#
# The example closes three times. Once with time to spare, so the job being
# worked on is finished and acknowledged. Once with a deadline shorter than the
# job, so close stops waiting, raises `DrainTimeout` naming what it left, and the
# job goes back to the broker for whoever comes next. And once with three busy
# consumers on one connection, to show that the deadline is one for all of them,
# not one each.

require "acemq/amqp"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

QUEUE = "rb-shutdown-jobs"

# Longer than the short deadline below, and nowhere near the default one.
JOB = 3.0

# Shorter than the job, which is the whole point of the second and third close.
SHORT = 0.5

# How far past SHORT a close may run and still count as having kept it.
SLACK = 0.5

# What each handler got to, in order. Handlers run on the transport's threads,
# so the log has a lock.
class Log
  def initialize
    @lock = Mutex.new
    @entries = []
  end

  def <<(entry)
    @lock.synchronize { @entries << entry }
  end

  def entries = @lock.synchronize { @entries.dup }

  def include?(entry) = entries.include?(entry)
end

def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

def wait_until(what, within: 15)
  deadline = now + within
  sleep 0.02 until yield || now > deadline
  abort "gave up waiting: #{what}" unless yield
end

def time
  started = now
  yield
  now - started
end

# A pod: a connection with `consumers` consumers working through jobs slowly.
def pod(name, log, consumers: 1)
  mq = Connection.open(URL, origin: "examples/11-graceful-shutdown/#{name}")
  consumers.times do
    mq.consume(QUEUE, prefetch: 1) do |message|
      job = message.payload["job"]
      log << [:started, job]
      sleep JOB
      log << [:finished, job]
      Ack.accept
    end
  end
  mq
end

# Closes with a deadline, returning how long it took and what it raised.
def close_within(connection, timeout)
  raised = nil
  took = time do
    connection.close(timeout: timeout)
  rescue Connection::DrainTimeout => e
    raised = e
  end
  [took, raised]
end

# Takes back what a close left on the queue, as the next reader would.
def take_back(setup, count)
  taken = []
  wait_until("#{count} redelivered job(s)") do
    while (delivery = setup.pull(QUEUE))
      delivery.ack
      taken << [delivery.body, delivery.redelivered?]
    end
    taken.size >= count
  end
  taken
end

setup = Connection.open(URL, origin: "examples/11-graceful-shutdown")
Topology.new.queue(QUEUE, dead_letter: true).apply(setup)

log = Log.new
failures = []

# ---------------------------------------------------------------------------
# Enough time. The default deadline is twenty seconds and the job takes three,
# so close waits for it, the job is acknowledged, and close returns.
first = pod("first", log)
setup.publish({ "job" => "J-1" }, to: QUEUE)
wait_until("J-1 to start") { log.include?([:started, "J-1"]) }

enough = time { first.close }
puts format("%-13s close took %.2fs and returned", "enough time", enough)
failures << "close returned before J-1 was finished" unless log.include?([:finished, "J-1"])
unless enough.between?(JOB * 0.8, JOB + 5)
  failures << "close took #{enough.round(2)}s for a #{JOB}s job"
end

# ---------------------------------------------------------------------------
# Not enough. The same kind of job against half a second. Close gives up at the
# deadline, shuts the socket anyway, and raises, saying what it left behind.
second = pod("second", log)
setup.publish({ "job" => "J-2" }, to: QUEUE)
wait_until("J-2 to start") { log.include?([:started, "J-2"]) }

short, raised = close_within(second, SHORT)
puts format("%-13s close(timeout: %s) took %.2fs and %s", "not enough", SHORT, short,
            raised ? "raised" : "returned")
puts "              #{raised.message}" if raised
failures << "close(timeout: #{SHORT}) did not raise DrainTimeout" unless raised
failures << "close(timeout: #{SHORT}) took #{short.round(2)}s" if short > SHORT + SLACK
failures << "stranded was #{raised&.stranded.inspect}" unless raised&.stranded == { QUEUE => 1 }

back = take_back(setup, 1)
back.each { |body, again| puts format("%-13s %s came back, redelivered=%s", "", body, again) }
unless back.any? { |body, again| body.include?("J-2") && again }
  failures << "J-2 did not come back redelivered"
end

# ---------------------------------------------------------------------------
# Three busy consumers, one deadline. Waited for one after another this would be
# a second and a half; shared, it is half a second.
third = pod("third", log, consumers: 3)
jobs = %w[J-3 J-4 J-5]
jobs.each { |job| setup.publish({ "job" => job }, to: QUEUE) }
wait_until("all three to start") { jobs.all? { |job| log.include?([:started, job]) } }

shared, raised = close_within(third, SHORT)
puts format("%-13s close(timeout: %s) took %.2fs and %s", "three busy", SHORT, shared,
            raised ? "raised" : "returned")
puts "              #{raised.message}" if raised
failures << "close of three did not raise DrainTimeout" unless raised
if shared > SHORT + SLACK
  failures << "close of three took #{shared.round(2)}s, not one shared deadline"
end
failures << "stranded was #{raised&.stranded.inspect}" unless raised&.stranded == { QUEUE => 3 }

back = take_back(setup, 3)
back.each { |body, again| puts format("%-13s %s came back, redelivered=%s", "", body, again) }
unless back.size == 3 && back.all? { |_, again| again }
  failures << "not all three came back redelivered"
end

# ---------------------------------------------------------------------------
# The abandoned handlers are not killed; they run to the end in this process and
# what they return is thrown away, because the broker already has the message
# back. Wait for them, then check none of them settled anything.
stranded_jobs = ["J-2", *jobs]
wait_until("the abandoned handlers to finish") do
  stranded_jobs.all? { |job| log.include?([:finished, job]) }
end
sleep 0.2
puts
puts "what the handlers got to:"
log.entries.each { |what, job| puts format("  %-4s %s", job, what) }

left = setup.message_count(QUEUE)
dead = setup.message_count(Naming.dead_letter_queue(QUEUE))
puts
puts "left on the queue: #{left}, dead-lettered: #{dead}"
failures << "an abandoned handler's outcome was not discarded" unless left.zero? && dead.zero?

[QUEUE, Naming.dead_letter_queue(QUEUE), Naming.parked_queue(QUEUE)].each do |name|
  setup.delete_queue(name) if setup.queue_exists?(name)
end
setup.close

abort failures.map { |f| "FAILED: #{f}" }.join("\n") unless failures.empty?
