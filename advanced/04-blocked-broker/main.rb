# frozen_string_literal: true

# A broker that has run out of memory, and a readiness probe that gets it right.
#
# RabbitMQ protects itself. Over its memory or disk high watermark it raises an
# alarm and stops reading from every connection that publishes: `connection.blocked`
# goes out, and from then on a publish, a declaration, or anything else written to
# that socket waits. Nothing fails. Nothing arrives either.
#
# From this end that is indistinguishable from a broker that has gone away, and
# the two want opposite responses. A blocked broker is one to wait for; a dead one
# is one to fail over from. A service that fails its own readiness check on a block
# is one an orchestrator restarts into the same blocked broker, having thrown away
# whatever it was holding — and doing that to every replica at once turns a broker
# under memory pressure into an outage with a crash loop on top.
#
# So a blocked connection reports `:up`, with the broker's reason written on it.
# That is a change rather than a new feature: the same connection reported
# `:degraded` up to 0.6.0.
#
# The second half of it is the timing. `mq.health` proves the connection by
# declaring a queue and deleting it again, and that round trip is exactly what a
# blocked broker will not complete — it does not fail, it hangs, until bunny's
# continuation timeout gives up seconds later and reports `:down` for a broker that
# is up and talking. So the block is read first, off the notification the broker
# already sent over the same socket, and the declare is skipped. `round_trip_ms` is
# absent from the parts because nothing was timed.
#
# This example has a broker to itself, on 5673. An alarm is broker-wide: dropping
# the watermark on the shared broker would stop every other example publishing, not
# only this one. The watermark is put back in an `ensure`, because a broker left
# with an alarm on it is a broker the next run inherits.

require "open3"

require "acemq/amqp"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_BLOCKED_URL", "amqp://guest:guest@localhost:5673")

# Nothing an AMQP client sends can raise or clear an alarm — it is the broker's
# decision about the broker's memory — so provoking one means reaching the broker
# the way an operator would, with `rabbitmqctl` inside its container.
CONTAINER = ENV.fetch("ACEMQ_BLOCKED_CONTAINER", "acemq-ruby-examples-blocked-broker")

QUEUE = "warehouse.blocked-picks"

# Only a fallback. The watermark in force is read off the broker and put back
# afterwards, because a number written down here would be wrong on half the
# brokers this library supports: RabbitMQ 3.13 defaults to 0.4 and 4.x to 0.6.
# When the broker will not say, the lower of the two is the safer guess — a
# broker restored too low is merely cautious, one restored too high has stopped
# protecting itself.
DEFAULT_WATERMARK = "0.4"

# One `rabbitmqctl` call, inside the broker's own container.
#
# `-u rabbitmq` and the `HOME` are not decoration. rabbitmqctl reaches the server
# over Erlang distribution and authenticates with the cookie under HOME; run as
# root it reads root's, is answered "Invalid challenge reply", and fails against a
# broker that is working perfectly.
def rabbitmqctl(*arguments)
  out, err, status = Open3.capture3("docker", "exec", "-u", "rabbitmq",
                                    "-e", "HOME=/var/lib/rabbitmq",
                                    CONTAINER, "rabbitmqctl", *arguments)
  unless status.success?
    abort "rabbitmqctl #{arguments.join(" ")} failed in #{CONTAINER}: " \
          "#{(err.empty? ? out : err).strip}\n" \
          "Start this example's own broker with `docker compose up -d --wait`, or name " \
          "another with ACEMQ_BLOCKED_CONTAINER and ACEMQ_BLOCKED_URL."
  end
  out.strip
rescue Errno::ENOENT
  abort "docker is not on PATH, and this example provokes a memory alarm with " \
        "rabbitmqctl inside the broker's container"
end

# What the broker's memory high watermark is now, to put back afterwards.
#
# `eval` rather than parsing `status`, because the wanted value is one term and
# `status` is two hundred lines of report around it.
def current_watermark
  value = rabbitmqctl("eval", "vm_memory_monitor:get_vm_memory_high_watermark().")
  value.empty? ? DEFAULT_WATERMARK : value
end

# One health check, timed. Timed here rather than inside the library because it is
# the caller's question: a readiness endpoint's budget is the probe's interval, and
# what matters is how long the answer took to reach the endpoint.
def timed_health(connection)
  started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  report = connection.health
  [report, (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1_000_000]
end

def show(label, report, microseconds)
  puts format("%s: %s in %dus", label, report.status, microseconds)
  puts "  #{report.detail}" unless report.detail.to_s.empty?
  report.parts.each { |name, value| puts "  #{name}: #{value.inspect}" }
end

watermark = current_watermark

mq = Connection.open(URL, origin: "examples/04-blocked-broker")
mq.delete_queue(QUEUE) if mq.queue_exists?(QUEUE)
Topology.new.queue(QUEUE).apply(mq)

# One publish before the alarm, and not only to have something on the queue.
# RabbitMQ blocks the connections that publish and leaves the ones that only
# consume alone, so a connection that had never published would sit through the
# entire alarm correctly reporting itself unblocked — true, and not what this is
# about.
mq.publish({ "pick_id" => "before the alarm" }, to: QUEUE)

before, before_us = timed_health(mq)
show("before the alarm", before, before_us)

puts
puts "setting the memory high watermark to 0 on #{CONTAINER}"
rabbitmqctl("set_vm_memory_high_watermark", "0")

# A thread, because a publish on a blocked connection does not raise — it sits in
# `wait_for_confirms` until the broker starts reading the socket again. That is
# the state being demonstrated, so it cannot also be something the main thread
# waits on. The thread is stopped and joined below, once the alarm is off.
confirmed = Thread::Queue.new
stop = false
publishing = Thread.new do
  40.times do |n|
    break if stop

    mq.publish({ "pick_id" => "during the alarm #{n}" }, to: QUEUE)
    confirmed << n
    sleep 0.2
  end
rescue StandardError => e
  warn "the publishing thread gave up: #{e.class}: #{e.message}"
end

during = nil
during_us = nil
begin
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 30
  sleep 0.1 until mq.blocked? || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

  unless mq.blocked?
    abort "the broker never blocked the connection; check that #{CONTAINER} is the " \
          "broker #{URL} points at"
  end

  puts "blocked: #{confirmed.size} publishes confirmed since the alarm, and one still waiting"
  during, during_us = timed_health(mq)
  show("while blocked", during, during_us)
ensure
  # In an `ensure` and not at the end of the happy path. Every check below is
  # about a broker under an alarm, so every one of them is a way to leave here
  # with the alarm still on — and the next thing to use this broker would find it
  # blocked for a reason nothing in its own output explains.
  stop = true
  puts
  puts "putting the watermark back to #{watermark}"
  rabbitmqctl("set_vm_memory_high_watermark", watermark)
end

# The alarm clears on the broker's next memory reading rather than on the command
# returning, and `connection.unblocked` arrives after that.
clear_by = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 30
sleep 0.1 while mq.blocked? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < clear_by

# The publish that was waiting is confirmed now the socket is being read again,
# which is the point of a block not being an error: it was never lost, only
# paused. Joined before anything else touches the connection, so that the health
# check below and a publish in flight are not on it at the same time.
publishing.join(60)
went_through = confirmed.size
puts "#{went_through} #{went_through == 1 ? "publish" : "publishes"} made it through in the " \
     "end, including the one that waited"

after, after_us = timed_health(mq)
show("after the alarm cleared", after, after_us)

puts
puts "on the queue: #{mq.message_count(QUEUE)}"

mq.delete_queue(QUEUE)
mq.close

abort "the connection was not up before the alarm: #{before.status}" unless before.up?
abort "blocked was reported before the alarm: #{before.parts["blocked"].inspect}" if
  before.parts["blocked"]
abort "the report before the alarm did not make a round trip" unless
  before.parts.key?("round_trip_ms")

# The behaviour change itself. This reported :degraded up to 0.6.0, and a
# readiness endpoint keyed on :up would have failed on a broker doing exactly what
# it is supposed to do under pressure.
abort "a blocked connection reported #{during.status}, not up" unless during.up?
abort "the blocked report does not carry the state" unless during.parts["blocked"] == true
abort "the blocked report has no reason on it" if during.parts["blocked_reason"].to_s.empty?

# The exact sentence, because it is the same sentence in Python, Java, Go and
# .NET: one alert rule reads a blocked broker whatever the service is written in,
# so the wording is a contract rather than a phrasing.
abort "the blocked detail is not the shared sentence: #{during.detail.inspect}" unless
  during.detail.start_with?(Health::BLOCKED)

abort "the blocked report made a round trip, which a blocked broker cannot answer" if
  during.parts.key?("round_trip_ms")

# Generous next to the microseconds this actually takes, and deliberately so: what
# it is here to catch is a check that went back to declaring against a blocked
# broker, and that one costs bunny's continuation timeout.
abort "the blocked report took #{(during_us / 1000).round}ms, so it waited on something" if
  during_us > 250_000

abort "the connection did not recover: #{after.status}" unless after.up?
abort "blocked was still set after the alarm cleared" if after.parts["blocked"]
abort "the report after the alarm did not make a round trip" unless
  after.parts.key?("round_trip_ms")
