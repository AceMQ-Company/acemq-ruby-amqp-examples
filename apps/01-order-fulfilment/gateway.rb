# frozen_string_literal: true

require "securerandom"
require "sqlite3"
require "acemq/amqp"
require "acemq/amqp/patterns"
require_relative "contracts"

module Fulfilment
  # Where orders enter the system.
  #
  # The edge of a system is where the dual-write problem lives: an order has to
  # be saved *and* announced, and doing those as two writes means a crash
  # between them either loses the announcement or announces something that was
  # never saved. Neither is recoverable by retrying, because the process that
  # would retry is the one that died.
  #
  # So the gateway does one write. The event is inserted in the same
  # transaction as the order, and a relay publishes it afterwards.
  class Gateway
    include AceMQ::AMQP

    # @param database [String] a path to this service's own SQLite file
    def initialize(url, database:, telemetry: nil)
      @mq = Connection.open(url, origin: "fulfilment-gateway", telemetry: telemetry)
      # Every service applies the whole topology. Applying it five times is
      # safe and means there is no deployment order to get wrong.
      Fulfilment.topology.apply(@mq)

      @db = open_database(database)
      @db.execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS orders (
          id TEXT PRIMARY KEY, customer TEXT, sku TEXT,
          quantity INTEGER, total NUMERIC, status TEXT)
      SQL

      # The relay gets a connection of its own, because it runs on its own
      # thread and must never find itself inside somebody's order transaction.
      # Sharing one SQLite handle would put the relay's mark-as-published in
      # the middle of whatever the request thread had open.
      @outbox = Patterns::SQLOutboxStore.new(connection: @db,
                                             relay: open_database(database),
                                             lease: 30)
      @outbox.create_schema
      @relay = Patterns::OutboxRelay.new(
        @mq, @outbox, interval: 0.2, batch: 20,
                      on_error: ->(e) { warn "gateway: outbox relay: #{e.message}" }
      ).start
    end

    # Takes an order.
    #
    # In a real gateway this is the body of an HTTP handler. The two writes
    # look exactly like this.
    #
    # @return [String] the id the customer is given
    def place_order(customer, sku, quantity, total)
      order_id = "ord-#{SecureRandom.uuid[0, 8]}"
      event = OrderPlaced.new(order_id: order_id, customer: customer, sku: sku,
                              quantity: quantity, total: total)

      @db.transaction do
        @db.execute("INSERT INTO orders VALUES (?, ?, ?, ?, ?, 'PLACED')",
                    [order_id, customer, sku, quantity, total])
        # The outbox writes through this connection, inside this transaction.
        # That is the whole trick: there is no second commit that can fail on
        # its own. The record holds encoded bytes rather than an object, so the
        # relay republishes exactly what was written here.
        @outbox.add(Patterns.record(@mq, event.to_wire,
                                    to: ORDER_PLACED, exchange: EXCHANGE,
                                    id: order_id, correlation_id: order_id,
                                    type: OrderPlaced.type),
                    connection: @db)
      end
      order_id
    end

    def pending_in_outbox = @outbox.pending_count

    def close
      @relay.close
      @mq.close
    end

    private

    # A SQLite file opened the way two threads can share: WAL so a reader does
    # not wait for a writer, and a busy handler that releases Ruby's lock while
    # it waits. Plain +busy_timeout+ holds it, so the thread holding the
    # database lock cannot run to release it and the waiter times out.
    def open_database(path)
      SQLite3::Database.new(path).tap do |db|
        db.busy_handler_timeout = 5_000
        db.execute("PRAGMA journal_mode = WAL")
      end
    end
  end
end
