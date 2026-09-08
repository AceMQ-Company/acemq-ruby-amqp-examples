# frozen_string_literal: true

# The three questions somebody asks about a running service, and where each
# answer comes from: how much is moving (metrics), whether it is working
# (health), and which message it was (a trace).
#
# Counters say how many messages failed. A trace says which one. They are not
# alternatives, and this example runs all three over one connection.

require "acemq/amqp"
require "opentelemetry/sdk"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "warehouse-events"
QUEUE = "warehouse.picks"

# ---------------------------------------------------------------------------
# Tracing. In a service the exporter is OTLP and the collector is somewhere
# else; here it is in memory, because the point of the example is to show the
# spans that were actually recorded rather than to prove a network hop.
#
# The leading `::` is not redundant, whatever RuboCop thinks: this file has
# `include AceMQ::AMQP` at the top, and that namespace has an `OpenTelemetry`
# of its own — the adapter, a few lines below. Writing the vendor's name
# unqualified here would be ambiguous to a reader even where it resolves.
# rubocop:disable Style/RedundantConstantBase
spans = ::OpenTelemetry::SDK::Trace::Export::InMemorySpanExporter.new
::OpenTelemetry::SDK.configure do |config|
  config.service_name = "warehouse"
  config.add_span_processor(
    ::OpenTelemetry::SDK::Trace::Export::SimpleSpanProcessor.new(spans)
  )
end
# rubocop:enable Style/RedundantConstantBase

# An observer is anything answering `count`, `observe` and `gauge`. There is no
# dependency on a metrics gem, because depending on one would put every service
# using this library on the same one, and that choice belongs to the
# application. `Telemetry::Registry` is a working in-memory implementation for
# when the numbers themselves are what is wanted.
metrics = Telemetry::Registry.new

mq = Connection.open(URL, origin: "examples/03-observability",
                          telemetry: metrics,
                          retry_policy: RetryPolicy.exponential(2, 0.1))

# Registers on both sides of the connection. The consumer span's parent comes
# out of the message's own headers rather than out of ambient context — that
# join, across processes and minutes, is the entire point of tracing a message
# system. The trace travels in `traceparent` and `tracestate`, the W3C names,
# deliberately not `x-acemq-` prefixed: other tooling already knows them, and
# the Java library writes the same two.
tracing = Telemetry::OpenTelemetry.new
tracing.install(mq)

[QUEUE, Naming.dead_letter_queue(QUEUE)].each do |queue|
  mq.delete_queue(queue) if mq.queue_exists?(queue)
end

Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(QUEUE, dead_letter: true)
        .binding(QUEUE, EXCHANGE, "pick.#")
        .apply(mq)

handled = Thread::Queue.new

consumer = mq.consume(QUEUE) do |message|
  handled << message.payload["pick_id"]
  # One of them fails outright, so there is something in the counters and
  # something red in the trace.
  next Ack.reject(FatalError.new("nothing on that shelf")) if message.payload["shelf"] == "gone"

  Ack.accept
end

3.times do |n|
  mq.publish({ "pick_id" => "P-#{n + 1}", "shelf" => n == 1 ? "gone" : "A-#{n}" },
             to: "pick.requested", exchange: EXCHANGE, type: "pick.requested.v1")
end

seen = 0
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 20
while seen < 3 && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
  begin
    handled.pop(true)
    seen += 1
  rescue ThreadError
    sleep 0.05
  end
end

# The dead letter is written after the handler returns, so give it a moment
# before reading the counters that record it.
sleep 0.3

# ---------------------------------------------------------------------------
# Health. The broker is checked by declaring a queue and deleting it again,
# because that is the cheapest thing AMQP offers that actually proves the round
# trip: an open socket answers the same as a healthy broker right up until
# something is asked of it. It costs a round trip, so it belongs on a readiness
# probe's interval rather than in a request.
healthy = mq.health
puts "health while consuming: #{healthy.status}"
healthy.to_h["parts"].each { |name, value| puts "  #{name}: #{value}" }
puts

consumer.cancel

# The same connection with nothing reading its queue. A consumer that has
# stopped under a live connection is `degraded`, not `down`: the process can
# still publish and its other consumers still work, so failing the probe would
# take out something doing most of its job — but a queue with nothing reading
# it is a real fault and has to be visible.
stopped = mq.health
puts "health after cancelling the consumer: #{stopped.status}"
puts "  #{stopped.detail}"
puts

# ---------------------------------------------------------------------------
# Metrics. The names are shared with Java, Go, .NET and Python, so a dashboard
# or an alert written for one service reads the same against the next.
#
# `to_prometheus` is a string and not a Rack app on purpose — this library has
# no web framework and should not choose one. Serve it on a port the ingress
# does not publish: what a service publishes and how long its handlers take is
# more than an anonymous caller should be able to learn.
puts "metrics:"
metrics.to_prometheus.lines.grep(/^acemq/).each { |line| puts "  #{line.chomp}" }
puts

# ---------------------------------------------------------------------------
# Spans. `<destination> publish` is a PRODUCER span and `<queue> process` is a
# CONSUMER one. `unroutable`, `failed` and `dead_lettered` make a span an
# error; `acked`, `retried` and `rejected` do not — a message that will be
# tried again has not failed yet, and a trace view where every retry is red
# stops meaning anything.
recorded = spans.finished_spans
puts "spans:"
recorded.sort_by(&:name).each do |span|
  puts format("  %-28s %-8s %s", span.name, span.kind,
              span.attributes["messaging.acemq.outcome"])
end

mq.close

abort "expected 3 messages, got #{seen}" unless seen == 3
abort "the health report was not up: #{healthy.status}" unless healthy.up?
abort "a stopped consumer should be degraded, not #{stopped.status}" unless stopped.degraded?

published = metrics.counts.find { |key, _| key.to_s.start_with?("acemq.messages.published") }
abort "nothing was counted as published" unless published && published.last >= 3

dead = metrics.counts.find { |key, _| key.to_s.start_with?("acemq.messages.dead.lettered") }
abort "the rejected pick was not counted as dead-lettered" unless dead && dead.last == 1

abort "no spans were recorded" if recorded.empty?
abort "no publish span" unless recorded.any? { |span| span.name.end_with?("publish") }
abort "no process span" unless recorded.any? { |span| span.name.end_with?("process") }

# The join is the claim worth checking: the consumer span's trace identifier is
# the publisher's, because it was read out of the message rather than started
# afresh. Without that, a trace stops at the broker and the two halves of a
# request are two unrelated traces.
publishes = recorded.select { |span| span.name.end_with?("publish") }.map(&:trace_id)
processes = recorded.select { |span| span.name.end_with?("process") }.map(&:trace_id)
abort "the consumer spans did not join the publisher's traces" unless
  (processes - publishes).empty?
