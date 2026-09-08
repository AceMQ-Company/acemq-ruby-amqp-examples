# frozen_string_literal: true

# A tenant stamped on every message and every handler timed, without either
# appearing in the handler — and a message from the wrong tenant stopped before
# it is sent at all.
#
# These are the things every message in an organisation needs and no library
# can guess. Without a seam for them they end up copied into every call site,
# where one of them is always the one that forgot.

require "acemq/amqp"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "support-events"
QUEUE = "support.tickets"

# Whatever your framework keeps per request. The point of the interceptor is
# that the handler never has to reach for it.
Current = Struct.new(:tenant).new("acme")

mq = Connection.open(URL, origin: "examples/04-interceptors")

[QUEUE, Naming.dead_letter_queue(QUEUE)].each do |queue|
  mq.delete_queue(queue) if mq.queue_exists?(queue)
end

Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(QUEUE, dead_letter: true)
        .binding(QUEUE, EXCHANGE, "ticket.#")
        .apply(mq)

# A block is the common case. A `PublishContext` is the message before it is
# encoded — exchange, routing key, envelope and payload — and every one of them
# can be changed, so an interceptor can redirect a message as well as decorate
# it, and can rewrite the payload while it is still a Ruby object rather than
# patching bytes afterwards.
mq.intercept_publish { |context| context.set_header("tenant", Current.tenant) }

# Raising from `before_publish` stops the publish and the caller sees the
# exception. That is the point of intercepting rather than observing: a message
# that must not go out is stopped once, here, rather than in every publisher.
mq.intercept_publish do |context|
  raise "refusing to publish a ticket with no subject" if context.payload["subject"].to_s.empty?
end

# An object is the full form, answering whichever hooks it cares about. Lower
# `order` runs first, and the way out of a handler is reversed, so a pair that
# opens something on the way in and closes it on the way out nests properly.
class Timing
  attr_reader :durations

  def initialize
    @durations = []
    @started = {}
    @lock = Mutex.new
  end

  def order = -100

  def before_handle(context)
    @lock.synchronize { @started[context.envelope.id] = now }
  end

  def after_handle(context, ack)
    started = @lock.synchronize { @started.delete(context.envelope.id) }
    return unless started

    tenant = context.envelope.headers["tenant"]
    @lock.synchronize { @durations << [tenant, ack.to_s, now - started] }
  end

  private

  def now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

timing = Timing.new
mq.intercept_consume(timing)

handled = Thread::Queue.new

consumer = mq.consume(QUEUE) do |message|
  # No tenant anywhere in here, and no timing either. That is the whole claim.
  handled << message.payload["ticket_id"]
  sleep 0.01
  Ack.accept
end

mq.publish({ "ticket_id" => "T-1", "subject" => "the printer is on fire" },
           to: "ticket.opened", exchange: EXCHANGE, type: "ticket.opened.v1")

refused = nil
begin
  mq.publish({ "ticket_id" => "T-2", "subject" => "" },
             to: "ticket.opened", exchange: EXCHANGE, type: "ticket.opened.v1")
rescue RuntimeError => e
  refused = e
end

seen = nil
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
until seen || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
  begin
    seen = handled.pop(true)
  rescue ThreadError
    sleep 0.05
  end
end

consumer.cancel

puts "handled:  #{seen}"
puts "refused:  #{refused&.message}"
timing.durations.each do |tenant, outcome, seconds|
  puts format("timed:    tenant=%s outcome=%s %.1fms", tenant, outcome, seconds * 1000)
end

# `set_header` refuses the reserved `x-acemq-` names outright, on a fresh
# connection so the refusal is the only thing being shown. Silently dropping a
# header somebody deliberately set would be worse than saying no, and letting
# one through would let an interceptor rewrite the attempt counter the retry
# engine is counting on.
reserved = nil
other = Connection.open(URL, origin: "examples/04-interceptors")
other.intercept_publish { |context| context.set_header("x-acemq-attempt", 99) }
begin
  other.publish({ "ticket_id" => "T-3", "subject" => "ignored" },
                to: "ticket.opened", exchange: EXCHANGE)
rescue ArgumentError => e
  reserved = e
end
other.close
mq.close

puts "reserved: #{reserved&.message}"

abort "nothing was handled" unless seen == "T-1"
abort "the interceptor did not stop the empty ticket" unless refused
abort "the handler was not timed" if timing.durations.empty?
abort "a reserved header was accepted" unless reserved

# The tenant reached the handler's side of the connection without the handler
# asking for it — and it is on the envelope any dead letter would be written
# with too, which is exactly when somebody goes looking for it.
tenant = timing.durations.first.first
abort "the tenant header did not survive: #{tenant.inspect}" unless tenant == "acme"
