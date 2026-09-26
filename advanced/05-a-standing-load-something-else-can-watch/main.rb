# A load that keeps running and says what is happening to it, one line at a time.
#
# Every other example here finishes. This one does not: it publishes and consumes at
# a steady rate and prints one JSON object per second describing what it has seen.
# That makes it the thing a fault drill breaks the cluster underneath — the drill
# kills a node or raises a memory alarm, reads these lines, and judges what the
# client did about it.
#
# Why a drill needs this rather than a probe of its own
# -----------------------------------------------------
# A probe that connects to the broker can answer "is the cluster usable". It cannot
# answer what an application saw: whether it was told the broker had stopped reading
# from it, whether it stopped publishing, whether it started again on its own or sat
# there. Those are properties of a client library, they differ between libraries
# that are otherwise equivalent, and the only thing that can report them is a
# client.
#
# What a line contains
# --------------------
#   blocked      the broker is refusing to read from this connection now
#   published    sends attempted since the start
#   confirmed    sends the broker has acknowledged
#   consumed     deliveries handled
#   failed       sends that failed
#   publishRate  confirms per second over the last interval
#   consumeRate  deliveries per second over the last interval
#
# Running it
# ----------
#   bundle exec ruby advanced/05-a-standing-load-something-else-can-watch/main.rb \
#     > readings.jsonl
#
# Then read the last few lines at any point to see what the client is seeing. Under
# a fault drill that file is the client's testimony, and `tail -n 60` on it is how
# the drill asks.
#
# It stops on Ctrl-C, or after ACEMQ_EXAMPLE_SECONDS if that is set — which CI sets,
# because a load with no reason to stop is not a failing example there, it is a job
# that never ends. Unset, it runs until interrupted, which is what a drill campaign
# wants.
#
# What to watch under a fault: `published` and `confirmed` moving apart, `blocked`
# turning true with the broker's reason beside it, and both counters moving again
# once it clears. None of that is visible to anything that asks the broker how it is
# doing — the cluster is healthy and this application is not publishing.

# frozen_string_literal: true

require "json"
require "timeout"
require "acemq/amqp"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage -- an example reads better unqualified

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672").freeze

# Quorum by default in this library, and the right choice here for the reason a
# drill exists: a classic queue lives on one node, and when that node is the one the
# drill stops, the load stops with it — which reports a client that gave up when the
# truth is that the queue went away.
QUEUE = "warehouse.standing-load"

RATE = Integer(ENV.fetch("ACEMQ_LOAD_RATE", "200"))
INTERVAL = Float(ENV.fetch("ACEMQ_LOAD_INTERVAL", "1"))

# Bounded in CI, where every example is run with no arguments and nothing is standing
# by to interrupt one.
RUN_FOR = Integer(ENV.fetch("ACEMQ_EXAMPLE_SECONDS", "0"))

# Logs go to stderr so that stdout carries nothing but readings. A reader skips
# whatever is not a JSON object, so mixing them would work — and it would also mean
# every diagnostic line here had to stay un-JSON-like for ever, which is not a
# property anybody would remember to preserve.
warn "standing load: #{URL}, #{QUEUE} at #{RATE}/s"

mq = Connection.open(URL, origin: "examples/05-a-standing-load")
mq.declare_queue(QUEUE)

# A mutex rather than bare integers, because the consumer runs on bunny's own thread
# and the sampler on this one. Ruby's GVL makes a torn read unlikely rather than
# impossible, and "unlikely" is not a property to leave in a program whose whole
# output is numbers somebody will reason about.
counts = { published: 0, confirmed: 0, consumed: 0, failed: 0 }
lock = Mutex.new
bump = ->(key) { lock.synchronize { counts[key] += 1 } }

consumer = mq.consume(QUEUE, concurrency: 4) do |_message|
  bump.call(:consumed)
  :accept
end

stopping = false
# Trap rather than rescue Interrupt: the publisher thread and this one both have to
# come down, and the flag is how they agree to.
%w[INT TERM].each { |sig| Signal.trap(sig) { stopping = true } }

publisher = Thread.new do
  number = 0
  interval = 1.0 / [RATE, 1].max
  until stopping
    number += 1
    bump.call(:published)
    begin
      # A publish on a blocked connection does not raise — it waits, because the
      # broker has stopped reading the socket. Timeout::timeout is what makes a
      # reading arrive anyway: without it the sampler would wait with the publish,
      # and a client that went quiet looks exactly like a client that was never
      # running.
      Timeout.timeout(5) { mq.publish({ "pick_id" => "o-#{number}" }, to: QUEUE) }
      bump.call(:confirmed)
    rescue StandardError
      # Counted rather than hidden, and not fatal: a standing load reports what
      # happened to it and keeps going.
      #
      # A Timeout::Error is the expected one while the connection is blocked -- a
      # send that never completed is a fact about the run, and `blocked` on the same
      # line is what says why. It needs no branch of its own, being a StandardError
      # like the rest: anything that goes wrong here is counted the same way rather
      # than ending the load, because a load that stops on the first error stops
      # being a witness.
      bump.call(:failed)
    end
    sleep interval
  end
end

started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
last = started
last_confirmed = 0
last_consumed = 0

emit = lambda do
  now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  elapsed = now - last
  snapshot = lock.synchronize { counts.dup }

  reading = {
    "at" => Time.now.utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "elapsedMs" => ((now - started) * 1000).to_i,
    "blocked" => mq.blocked?,
    "published" => snapshot[:published],
    "confirmed" => snapshot[:confirmed],
    "consumed" => snapshot[:consumed],
    "failed" => snapshot[:failed]
  }
  reason = mq.blocked_reason
  reading["reason"] = reason if reason
  if elapsed.positive?
    reading["publishRate"] = (snapshot[:confirmed] - last_confirmed) / elapsed
    reading["consumeRate"] = (snapshot[:consumed] - last_consumed) / elapsed
  end

  last = now
  last_confirmed = snapshot[:confirmed]
  last_consumed = snapshot[:consumed]

  # Flushed, because stdout to a file is block-buffered and a drill reads that file
  # while this process is still running. Without the flush the last readings sit in a
  # buffer for minutes and the drill reports a client that went quiet.
  $stdout.puts(JSON.generate(reading))
  $stdout.flush
end

until stopping
  sleep INTERVAL
  emit.call
  next unless RUN_FOR.positive?

  stopping = true if Process.clock_gettime(Process::CLOCK_MONOTONIC) - started >= RUN_FOR
end

publisher.join(6)
consumer.cancel
mq.close
