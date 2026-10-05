# frozen_string_literal: true

require "acemq/amqp"
require_relative "contracts"

module Fulfilment
  # Holds stock for orders that have been paid for.
  #
  # The service that talks to something unreliable. A warehouse system that
  # times out is the ordinary case, not the exception, and two failures have to
  # be told apart:
  #
  # - the warehouse did not answer -- retry, it will probably work in a moment;
  # - there are three left and the order wants ten -- retrying changes nothing.
  #
  # The first is a plain exception and goes up the retry ladder. The second is
  # not an exception at all: it is an outcome, published as StockUnavailable so
  # the customer hears about it now rather than after four pointless attempts.
  class Inventory
    include AceMQ::AMQP

    attr_reader :reserved, :rejected

    def initialize(url, telemetry: nil)
      @mq = Connection.open(url, origin: "fulfilment-inventory", telemetry: telemetry)
      Fulfilment.topology.apply(@mq)

      @stock = {}
      @lock = Mutex.new
      @reserved = 0
      @rejected = 0
      @warehouse_calls = 0
      @failures_to_simulate = 0

      # Delays of 0.2s rising to 5s, all under the 30-second threshold, so the
      # wait happens in this consumer rather than in a broker rung. Java's
      # default threshold is the same thirty seconds and makes the same choice.
      ladder = RetryPolicy.exponential(4, 0.2, 5)
      @consumer = @mq.consume(INVENTORY, prefetch: 20, retry_policy: ladder) do |message|
        reserve(PaymentCaptured.from_wire(message.payload), message.envelope)
        Ack.accept
      end
    end

    def with_stock(sku, quantity)
      @lock.synchronize { @stock[sku] = quantity }
      self
    end

    # Makes the next +count+ warehouse calls fail, the way a real one does.
    def with_flaky_warehouse(count)
      @failures_to_simulate = count
      self
    end

    def stock_of(sku) = @lock.synchronize { @stock.fetch(sku, 0) }

    def close = @mq.close(timeout: 10)

    private

    def reserve(payment, envelope)
      # The transient failure. Nothing is wrong with the message, so it goes
      # back on the ladder and arrives again shortly, on its next attempt.
      @warehouse_calls += 1
      raise "warehouse did not respond" if @warehouse_calls <= @failures_to_simulate

      available = stock_of(payment.sku)
      if available < payment.quantity
        # The permanent one. Retrying will not conjure stock.
        publish(StockUnavailable.new(order_id: payment.order_id, customer: payment.customer,
                                     sku: payment.sku, reason: "only #{available} left"),
                STOCK_UNAVAILABLE, envelope)
        @rejected += 1
        return
      end

      @lock.synchronize { @stock[payment.sku] -= payment.quantity }
      publish(StockReserved.new(order_id: payment.order_id, customer: payment.customer,
                                sku: payment.sku, quantity: payment.quantity),
              STOCK_RESERVED, envelope)
      @reserved += 1
    end

    def publish(event, routing_key, cause)
      @mq.publish(event.to_wire, to: routing_key, exchange: EXCHANGE,
                                 type: event.class.type, correlation_id: cause.correlation_id)
    end
  end
end
