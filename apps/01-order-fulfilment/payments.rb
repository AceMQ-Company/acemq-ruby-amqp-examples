# frozen_string_literal: true

require "sqlite3"
require "acemq/amqp"
require "acemq/amqp/patterns"
require_relative "contracts"

module Fulfilment
  # Takes the money.
  #
  # This is the service where at-least-once delivery stops being a
  # technicality. Every other service here can handle a message twice and
  # produce the same outcome; this one cannot, because the second charge is
  # real money belonging to a real customer.
  #
  # So it claims each order in a shared store before charging, and confirms
  # afterwards. The store is a database rather than memory because there is
  # more than one instance of this service in production, and an in-memory
  # store makes each instance idempotent while the fleet is not.
  class Payments
    include AceMQ::AMQP

    # Over this, a human has to look at it. Every payment system has one.
    AUTOMATIC_LIMIT = 1_000.00

    attr_reader :captured, :declined, :duplicates_refused

    # @param database [String] a path to this service's own SQLite file
    def initialize(url, database:, telemetry: nil)
      @mq = Connection.open(url, origin: "fulfilment-payments", telemetry: telemetry)
      Fulfilment.topology.apply(@mq)

      # A generous claim timeout: it has to outlast the slowest charge, because
      # a claim that expires while the payment provider is still thinking is a
      # claim another instance will take, and then the customer pays twice.
      #
      # One connection, used only by the consumer's one thread. Concurrency 1 is
      # what makes that safe; raise it and give each worker its own.
      @charged = Patterns::SQLIdempotencyStore.new(
        connection: SQLite3::Database.new(database),
        retention: 7 * 24 * 3600, claim_timeout: 120, table: "payments_handled"
      )
      @charged.create_schema

      @captured = 0
      @declined = 0
      @duplicates_refused = 0

      ladder = RetryPolicy.exponential(4, 0.2, 5)
      @consumer = @mq.consume(PAYMENTS, prefetch: 20, retry_policy: ladder) do |message|
        charge(OrderPlaced.from_wire(message.payload), message.envelope)
        Ack.accept
      end
    end

    def close = @mq.close(timeout: 10)

    private

    def charge(order, envelope)
      # The claim is keyed by the order, not by the message. A redelivery -- a
      # broker restart, a consumer that died mid-charge, a relay that published
      # twice under a new message id -- carries the same order id and loses
      # here.
      unless @charged.first_time?(order.order_id)
        @duplicates_refused += 1
        return
      end

      begin
        if order.total > AUTOMATIC_LIMIT
          publish(PaymentDeclined.new(order_id: order.order_id, customer: order.customer,
                                      reason: "over the automatic limit"),
                  PAYMENT_DECLINED, envelope)
          @declined += 1
        else
          publish(PaymentCaptured.new(order_id: order.order_id, customer: order.customer,
                                      sku: order.sku, quantity: order.quantity,
                                      amount: order.total),
                  PAYMENT_CAPTURED, envelope)
          @captured += 1
        end
      rescue StandardError
        # Nothing was announced, so nothing was charged as far as the rest of
        # the system knows. Let the retry have the order.
        @charged.forget(order.order_id)
        raise
      end

      # Confirmed only after the outcome is published. Confirming first would
      # mean a crash in between leaves the order marked as charged with nothing
      # downstream ever told -- an order that took the money and stopped.
      @charged.confirm(order.order_id)
    end

    # The correlation id is what makes five services one story in a log
    # aggregator. Carrying it forward is not optional.
    def publish(event, routing_key, cause)
      @mq.publish(event.to_wire, to: routing_key, exchange: EXCHANGE,
                                 type: event.class.type,
                                 correlation_id: cause.correlation_id)
    end
  end
end
