# frozen_string_literal: true

require "acemq/amqp"
require_relative "contracts"

module Fulfilment
  # Tells the customer what happened.
  #
  # Bound to fulfilment.# -- everything. This is the service that shows why a
  # topic exchange is worth more than a queue per pair of services: it was added
  # without a single change to any publisher, and the next one will be too.
  #
  # It never reads a body. Six event types with six shapes arrive on one queue,
  # and the envelope already carries the two things this service needs: the type,
  # and the correlation id saying which order it belongs to. The text codec says
  # so out loud; the JSON default would also work here, because a Ruby consumer
  # gets a Hash whatever the shape, and that is the one place this service is
  # easier to write in Ruby than in Java.
  class Notifications
    include AceMQ::AMQP

    def initialize(url, telemetry: nil)
      @mq = Connection.open(url, origin: "fulfilment-notifications", telemetry: telemetry)
      Fulfilment.topology.apply(@mq)
      @timeline = Hash.new { |hash, order| hash[order] = [] }
      @lock = Mutex.new

      @consumer = @mq.consume(NOTIFICATIONS, prefetch: 50, codec: StringCodec.new) do |message|
        # The correlation id is the order it belongs to, set by whichever
        # service published it and carried forward by all of them.
        @lock.synchronize do
          @timeline[message.envelope.correlation_id] << message.envelope.type
        end
        Ack.accept
      end
    end

    # What a customer looking at "where is my order" would be shown.
    def timeline_of(order_id) = @lock.synchronize { @timeline.fetch(order_id, []).dup }

    def close = @mq.close(timeout: 10)
  end
end
