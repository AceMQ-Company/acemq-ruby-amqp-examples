# frozen_string_literal: true

# The whole system, five services, one broker.
#
# In production these are five deployments. Here they run in one process
# against one real RabbitMQ, which exercises every queue, every hop and every
# failure path for the cost of a single broker -- and fails if any service
# stops agreeing with the contracts.
#
# Four orders go through, each into a freshly started system: one that
# succeeds, one where the warehouse is flaky, one over the payment limit, and
# one where stock runs out. Every claim is checked, and the first that does
# not hold ends the run non-zero.

require "tmpdir"
require "acemq/amqp"
require_relative "contracts"
require_relative "gateway"
require_relative "payments"
require_relative "inventory"
require_relative "shipping"
require_relative "notifications"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

def check(claim, what)
  abort "FAILED: #{what}" unless claim
  puts "  ok  #{what}"
end

# Polls rather than sleeps, and waits for the final state rather than an
# intermediate one: waiting for "three events" can miss the moment the third
# arrives and the fourth follows, a flake that only shows on a fast machine.
def wait_for(what, seconds: 90)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
  until yield
    if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      abort "FAILED: the system did not reach the expected state in time: #{what}"
    end
    sleep 0.05
  end
end

# Whatever an earlier run left behind would turn up in this run's counters, so
# the queues start empty. Every service declares them again on start-up.
Connection.open(URL).tap do |mq|
  Fulfilment::QUEUES.each do |queue|
    [queue, Naming.dead_letter_queue(queue), Naming.parked_queue(queue)].each do |name|
      mq.delete_queue(name) if mq.queue_exists?(name)
    end
  end
  mq.close
end

# The five services of one run, and the telemetry the library reports into.
System = Struct.new(:gateway, :payments, :inventory, :shipping, :notifications, :telemetry,
                    keyword_init: true)

# Starts all five, runs one scenario, and stops them, whatever happened.
#
# A database per service, because services do not share one. The moment two
# services read the same table, the deployment boundary is a fiction.
def with_the_system(title)
  puts title
  Dir.mktmpdir("fulfilment") do |dir|
    system = System.new(telemetry: Telemetry::Registry.new)
    system.notifications = Fulfilment::Notifications.new(URL)
    system.shipping = Fulfilment::Shipping.new(URL)
    system.inventory = Fulfilment::Inventory.new(URL, telemetry: system.telemetry)
                                            .with_stock("WIDGET", 10)
    system.payments = Fulfilment::Payments.new(URL, database: File.join(dir, "payments.db"))
    system.gateway = Fulfilment::Gateway.new(URL, database: File.join(dir, "gateway.db"))
    yield system
  ensure
    # Gateway first, so nothing new enters while the rest drain.
    [system.gateway, system.payments, system.inventory, system.shipping,
     system.notifications].compact.each(&:close)
  end
  puts
end

with_the_system("an order travels through every service") do |s|
  order = s.gateway.place_order("ada", "WIDGET", 2, 42.00)
  wait_for("shipped") { s.shipping.shipped == 1 }

  # One order in at the gateway, and every service downstream acted once.
  check s.payments.captured == 1, "payments captured it once"
  check s.inventory.reserved == 1, "inventory reserved it once"
  check s.shipping.shipped == 1, "shipping shipped it once"
  # Stock actually moved. Without this the reservation is a log line.
  check s.inventory.stock_of("WIDGET") == 8, "stock went from 10 to 8"

  # The customer's view is the whole story, assembled from events published by
  # four services that never spoke to each other.
  wait_for("four events") { s.notifications.timeline_of(order).size >= 4 }
  timeline = s.notifications.timeline_of(order)
  puts "      #{timeline.join(" -> ")}"
  check timeline == %w[OrderPlaced PaymentCaptured StockReserved OrderShipped],
        "the timeline is built from the correlation id alone"
  check s.gateway.pending_in_outbox.zero?, "the outbox is empty"
  check s.payments.duplicates_refused.zero?, "the relay published the order once"
end

with_the_system("a flaky warehouse is retried rather than failed") do |s|
  s.inventory.with_flaky_warehouse(2)
  order = s.gateway.place_order("grace", "WIDGET", 1, 10.00)
  wait_for("shipped") { s.shipping.shipped == 1 }

  # Two failures, then success. The order was never lost and no human was
  # involved. The count is the library's own, read off its telemetry.
  retried = s.telemetry[Telemetry::RETRIED_TOTAL, queue: Fulfilment::INVENTORY,
                                                  outcome: Telemetry::Outcome::RETRIED]
  check retried >= 2, "inventory retried #{retried} times (at least 2)"
  check s.inventory.reserved == 1, "and reserved it once"
  wait_for("shipped notification") do
    s.notifications.timeline_of(order).include?("OrderShipped")
  end
  puts "  ok  the customer heard it shipped"
end

with_the_system("an order over the limit stops at payments") do |s|
  order = s.gateway.place_order("charles", "WIDGET", 1, 5_000.00)
  wait_for("declined") { s.payments.declined == 1 }

  # Nothing downstream ran, which is the point of declining before reserving:
  # stock held for an order that cannot be paid for is stock nobody releases.
  check s.inventory.reserved.zero?, "inventory reserved nothing"
  check s.shipping.shipped.zero?, "shipping shipped nothing"
  check s.inventory.stock_of("WIDGET") == 10, "stock is untouched"

  wait_for("two events") { s.notifications.timeline_of(order).size >= 2 }
  check s.notifications.timeline_of(order) == %w[OrderPlaced PaymentDeclined],
        "the timeline ends at the decline"
end

with_the_system("there is not enough stock, and retrying would not help") do |s|
  order = s.gateway.place_order("alan", "WIDGET", 99, 99.00)
  wait_for("rejected") { s.inventory.rejected == 1 }

  # The money was taken and the stock was not there. In a real system this is
  # where a refund is triggered; it is deliberately visible rather than
  # swallowed.
  check s.payments.captured == 1, "the customer was charged"
  check s.shipping.shipped.zero?, "nothing shipped"

  wait_for("three events") { s.notifications.timeline_of(order).size >= 3 }
  check s.notifications.timeline_of(order) == %w[OrderPlaced PaymentCaptured StockUnavailable],
        "the timeline shows the charge and the shortfall"
end

# Nothing in any of the four was given up on. A message sent to a dead-letter
# or parking queue fails none of the checks above -- the order just quietly is
# not where it was expected -- so the broker is asked directly.
puts "afterwards"
Connection.open(URL).tap do |mq|
  Fulfilment::QUEUES.flat_map { |q| [Naming.dead_letter_queue(q), Naming.parked_queue(q)] }
                    .each do |name|
    held = mq.queue_exists?(name) ? mq.message_count(name) : 0
    check held.zero?, "#{name} holds #{held}"
  end
  mq.close
end
