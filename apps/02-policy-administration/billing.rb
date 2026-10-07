# frozen_string_literal: true

require "acemq/amqp"
require "acemq/amqp/patterns"
require_relative "contracts"

module Policies
  # Taking the first premium: the one module where handling a message twice is
  # real money.
  #
  # Identical reasoning to payments in apps/01, and worth repeating because the
  # monolith makes it easy to assume the problem went away. It did not. A retry
  # still redelivers, a redeploy mid-handler still leaves a message
  # unacknowledged, and both still produce a second delivery of a message that
  # already took money.
  #
  # The idempotency store is SQL rather than in-memory even though this is one
  # process, because "one process" is a fact about today. The moment this module
  # is lifted out -- which is the whole point of the arrangement -- an in-memory
  # store becomes two stores that each think they are the only one.
  class BillingModule
    include AceMQ::AMQP

    # @param database [#call] opens a handle on the application's one database
    def initialize(connection, database:)
      @mq = connection
      @charges = []
      @lock = Mutex.new
      # Its own handle, used only by the consumer's one thread. The table lives
      # in the same database as the policies, which is the monolith's privilege.
      @seen = Patterns::SQLIdempotencyStore.new(connection: database.call)
      @seen.create_schema

      # The store wraps the handler rather than being used by hand: the claim is
      # taken before the handler and confirmed after it accepts, which is the
      # order that closes the window a manual "mark it afterwards" leaves open.
      # Keyed by message id, as Java's +idempotent(seen)+ is.
      @issued = @mq.consume(BILLING, prefetch: 10, &Patterns.idempotent(@seen) do |message|
        charge(PolicyIssued.from_wire(message.payload))
        Ack.accept
      end)
    end

    # One entry per charge actually taken; duplicates here would be the bug.
    def charges = @lock.synchronize { @charges.dup }

    def close = @issued.cancel

    private

    def charge(policy)
      @lock.synchronize { @charges << policy.policy_id }
      @mq.publish(PremiumCharged.new(policy_id: policy.policy_id, applicant: policy.applicant,
                                     amount: policy.annual_premium).to_wire,
                  to: PREMIUM_CHARGED, exchange: EXCHANGE, mandatory: true,
                  type: PremiumCharged.type, correlation_id: policy.application_id)
    end
  end
end
