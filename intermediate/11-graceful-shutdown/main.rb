# frozen_string_literal: true

# A shutdown that waits for the handler in hand — and, in 0.7.5, goes on waiting
# past the deadline it was given.
#
# Kubernetes sends SIGTERM and starts a clock. When it runs out the process is
# killed, and whatever is still inside a handler dies with it. `Connection#close`
# is the part of that a service controls: it stops every consumer, waits for the
# handlers already running, and only then shuts the socket — within one deadline,
# twenty seconds unless told otherwise.
#
# The example closes twice. Once with time to spare, so the job being worked on
# is finished and acknowledged. Once with a deadline shorter than the job, which
# is meant to stop waiting, raise `DrainTimeout` and leave the job for whoever
# comes next. In 0.7.5 it does not: bunny waits for the handler underneath the
# drain, for up to sixty seconds, whatever deadline the drain was given. The
# README has the detail. This example asserts what the release does, so the day
# that changes it fails here and gets rewritten rather than going on describing
# a library that no longer exists.

require "acemq/amqp"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

QUEUE = "rb-shutdown-jobs"

# Longer than the short deadline below, and nowhere near the default one.
JOB = 1.5

# Shorter than the job, which is the whole point of the second close.
SHORT = 0.5

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

# A pod: a connection with one consumer working through jobs slowly.
def pod(name, log)
  mq = Connection.open(URL, origin: "examples/11-graceful-shutdown/#{name}")
  mq.consume(QUEUE, prefetch: 1) do |message|
    job = message.payload["job"]
    log << [:started, job]
    sleep JOB
    log << [:finished, job]
    Ack.accept
  end
  mq
end

setup = Connection.open(URL, origin: "examples/11-graceful-shutdown")
Topology.new.queue(QUEUE, dead_letter: true).apply(setup)

log = Log.new

# ---------------------------------------------------------------------------
# Enough time. The default deadline is twenty seconds and the job takes one and
# a half, so close waits for it, the job is acknowledged, and close returns.
first = pod("first", log)
setup.publish({ "job" => "J-1" }, to: QUEUE)
wait_until("J-1 to start") { log.include?([:started, "J-1"]) }

enough = time { first.close }
finished_in_time = log.include?([:finished, "J-1"])
puts format("%-12s close took %.1fs and returned", "enough time", enough)

# ---------------------------------------------------------------------------
# Not enough. The same job against half a second. What a service is promised is
# a close that gives up at the deadline and raises; what 0.7.5 does is wait for
# the job anyway, because the cancel underneath it blocks until the handler is
# done.
second = pod("second", log)
setup.publish({ "job" => "J-2" }, to: QUEUE)
wait_until("J-2 to start") { log.include?([:started, "J-2"]) }

timeout = nil
short = time do
  second.close(timeout: SHORT)
rescue Connection::DrainTimeout => e
  timeout = e
end
puts format("%-12s close(timeout: %s) took %.1fs and %s", "not enough", SHORT, short,
            timeout ? "raised" : "returned")

puts
puts "what the handlers got to:"
log.entries.each { |what, job| puts format("  %-4s %s", job, what) }

# Whoever comes next — another pod, or this one restarted. Nothing was left,
# because the job the deadline should have abandoned was finished instead.
left = setup.pull(QUEUE)
left&.ack
puts
puts "left on the queue for the next reader: #{left ? left.body : "nothing"}"

[QUEUE, Naming.dead_letter_queue(QUEUE), Naming.parked_queue(QUEUE)].each do |name|
  setup.delete_queue(name) if setup.queue_exists?(name)
end
setup.close

# ---------------------------------------------------------------------------
abort "close returned before J-1 was finished" unless finished_in_time
abort "close returned in #{enough.round(2)}s, before the job could have finished" if
  enough < JOB * 0.8
abort "close with time to spare took #{enough.round(1)}s" if enough > JOB + 5

# What 0.7.5 does with a deadline shorter than the handler. Every one of these
# failing together means the drain has started honouring its deadline: the
# example and its README then describe the old library, and want rewriting to
# show DrainTimeout, its `stranded` count and the job coming back redelivered.
unless timeout.nil? && short >= JOB * 0.8
  abort "close(timeout: #{SHORT}) took #{short.round(1)}s and " \
        "#{timeout ? "raised #{timeout.message}" : "returned"} — the drain now honours " \
        "its deadline, which 0.7.5 did not. Rewrite this example and its README for it."
end
abort "J-2 was not finished, so the drain did stop waiting for it" unless
  log.include?([:finished, "J-2"])
abort "something was left on the queue: #{left.body}" if left
