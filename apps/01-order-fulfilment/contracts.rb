# frozen_string_literal: true

require "acemq/amqp"

# What every service in this system agrees on, and nothing else.
#
# The events, the exchange, the queue each service reads, and the routing keys
# that connect them. In a larger estate this is what a schema registry holds;
# here it is one file every service requires.
#
# What is deliberately *not* here: any service's domain model, any database
# access, any shared "helper". A contracts file that grows those stops being a
# contract and becomes a shared library, which is how five services turn back
# into one deployable that happens to have five entry points.
#
# Every name and every field is the one Java's `apps/01-order-fulfilment` uses,
# camelCase on the wire included. A contract is the part that is not allowed to
# be idiomatic.
module Fulfilment
  # One topic exchange. Every event in the system is published here.
  EXCHANGE = "fulfilment"

  # An event is a Struct, so it cannot carry a field the contract does not name.
  # Members are Ruby's snake_case; the wire is the contract's camelCase, and the
  # two conversions below are the only place that difference exists.
  module Event
    def self.define(*members)
      Struct.new(*members, keyword_init: true) do
        # The envelope type, which is the class name: "OrderPlaced".
        def self.type = name.split("::").last

        def self.from_wire(hash)
          new(**members.to_h { |m| [m, hash.fetch(Event.camel(m))] })
        end

        def to_wire = to_h.transform_keys { |k| Event.camel(k) }
      end
    end

    def self.camel(member) = member.to_s.gsub(/_(\w)/) { Regexp.last_match(1).upcase }
  end

  # Each carries the order id, because that is the only identifier every service
  # shares -- and correlation across five services is otherwise guesswork.

  # Someone placed an order. Published by the gateway, from its outbox.
  OrderPlaced = Event.define(:order_id, :customer, :sku, :quantity, :total)
  # The money is ours. Published by payments.
  PaymentCaptured = Event.define(:order_id, :customer, :sku, :quantity, :amount)
  # It is not, and will not be. Published by payments; nothing downstream proceeds.
  PaymentDeclined = Event.define(:order_id, :customer, :reason)
  # Stock is held for this order. Published by inventory.
  StockReserved = Event.define(:order_id, :customer, :sku, :quantity)
  # There is not enough. Published by inventory; the money must be given back.
  StockUnavailable = Event.define(:order_id, :customer, :sku, :reason)
  # On its way. Published by shipping.
  OrderShipped = Event.define(:order_id, :customer, :tracking)

  # "fulfilment.<aggregate>.<past-tense-verb>". The aggregate in the middle is
  # what lets a service subscribe to everything about orders without naming each
  # event, and lets notifications subscribe to everything at all.
  ORDER_PLACED = "fulfilment.order.placed"
  PAYMENT_CAPTURED = "fulfilment.payment.captured"
  PAYMENT_DECLINED = "fulfilment.payment.declined"
  STOCK_RESERVED = "fulfilment.stock.reserved"
  STOCK_UNAVAILABLE = "fulfilment.stock.unavailable"
  ORDER_SHIPPED = "fulfilment.order.shipped"

  # A queue per service, named after the service rather than after the event.
  # Two services wanting the same event each get their own copy, and neither can
  # starve the other.
  PAYMENTS = "fulfilment.payments"
  INVENTORY = "fulfilment.inventory"
  SHIPPING = "fulfilment.shipping"
  NOTIFICATIONS = "fulfilment.notifications"
  QUEUES = [PAYMENTS, INVENTORY, SHIPPING, NOTIFICATIONS].freeze

  # The whole system's topology, as one value.
  #
  # Every service applies this on start-up. Applying it from five places is safe
  # and is the point: no service depends on another having started first, and
  # there is no deployment order to get wrong.
  #
  # Classic queues, as Java declares them. A queue's type is part of its
  # identity to the broker, so a Ruby service and a Java one sharing these
  # queues have to agree or the second is refused with PRECONDITION_FAILED.
  def self.topology
    AceMQ::AMQP::Topology.new
                         .exchange(EXCHANGE, :topic)
                         # Payments acts on new orders.
                         .queue(PAYMENTS, queue_type: :classic)
                         .binding(PAYMENTS, EXCHANGE, ORDER_PLACED)
                         # Inventory acts once the money is taken, not before.
                         .queue(INVENTORY, queue_type: :classic)
                         .binding(INVENTORY, EXCHANGE, PAYMENT_CAPTURED)
                         # Shipping needs stock held.
                         .queue(SHIPPING, queue_type: :classic)
                         .binding(SHIPPING, EXCHANGE, STOCK_RESERVED)
                         # Notifications wants everything, which is what a
                         # wildcard is for.
                         .queue(NOTIFICATIONS, queue_type: :classic)
                         .binding(NOTIFICATIONS, EXCHANGE, "fulfilment.#")
  end
end
