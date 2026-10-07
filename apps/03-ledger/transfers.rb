# frozen_string_literal: true

require "securerandom"
require "acemq/amqp"
require_relative "contracts"

module Ledger
  # Where transfers are asked for, and where refusals are noticed.
  #
  # Deliberately thin. Everything a ledger is careful about happens in the
  # writer; this module makes the shape obvious -- a transfer is a *command*,
  # sent to a queue, which may be refused, and a refusal is a normal outcome
  # rather than an error.
  #
  # Events are named in the past tense and cannot be argued with; commands are
  # requests and can be turned down. Systems that blur the two end up publishing
  # +TransferMade+ before knowing whether it was, and then need a second event to
  # take it back.
  class TransferGateway
    include AceMQ::AMQP

    def initialize(connection)
      @mq = connection
      @refused = []
      @lock = Mutex.new
      @rejections = @mq.consume(REJECTIONS) do |message|
        rejected = TransferRejected.from_wire(message.payload)
        @lock.synchronize { @refused << rejected }
        Ack.accept
      end
    end

    # Asks for money to move.
    #
    # @return [String] the transfer id, which correlates the command with both
    #   entries and any refusal
    def request(from, to, amount_minor, description)
      transfer_id = "T-#{SecureRandom.uuid[0, 8]}"
      @mq.publish(Transfer.new(transfer_id: transfer_id, from: from, to: to,
                               amount_minor: amount_minor, description: description).to_wire,
                  to: TRANSFER_REQUESTED, exchange: EXCHANGE, mandatory: true,
                  type: Transfer.type, correlation_id: transfer_id)
      transfer_id
    end

    # The transfers the ledger refused, and why.
    def refused = @lock.synchronize { @refused.dup }

    def close = @rejections.cancel
  end
end
