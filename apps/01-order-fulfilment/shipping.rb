# frozen_string_literal: true

require "acemq/amqp"
require_relative "contracts"

module Fulfilment
  # Dispatches what has been paid for and reserved.
  #
  # The simplest service in the system, and it is worth noticing why: it reacts
  # to one event, does one thing, and publishes one event. It knows nothing
  # about payments, nothing about stock levels, and nothing about who else cares
  # that an order shipped.
  #
  # That is the property the whole architecture is buying. Adding a service
  # that also reacts to stock.reserved requires no change here at all.
  class Shipping
    include AceMQ::AMQP

    attr_reader :shipped

    def initialize(url, telemetry: nil)
      @mq = Connection.open(url, origin: "fulfilment-shipping", telemetry: telemetry)
      Fulfilment.topology.apply(@mq)
      @shipped = 0

      @consumer = @mq.consume(SHIPPING, prefetch: 10) do |message|
        dispatch(StockReserved.from_wire(message.payload), message.envelope)
        Ack.accept
      end
    end

    def close = @mq.close(timeout: 10)

    private

    def dispatch(reservation, envelope)
      tracking = "TRK-#{reservation.order_id[4..].upcase}"
      shipped = OrderShipped.new(order_id: reservation.order_id,
                                 customer: reservation.customer, tracking: tracking)
      @mq.publish(shipped.to_wire, to: ORDER_SHIPPED, exchange: EXCHANGE,
                                   type: OrderShipped.type,
                                   correlation_id: envelope.correlation_id)
      @shipped += 1
    end
  end
end
