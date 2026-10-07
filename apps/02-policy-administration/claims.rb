# frozen_string_literal: true

require "securerandom"
require "acemq/amqp"
require "acemq/amqp/patterns"
require_relative "contracts"

module Policies
  # Claims: the module that has to ask another module a question.
  #
  # Everything else here reacts to events, which is the right default. Claims
  # cannot: before settling a claim it must know whether the policy is in force,
  # and it needs the answer *now*, in the middle of a decision. An event cannot
  # answer a question.
  #
  # So it asks, over the broker. The callee is in the same process, and a
  # method call would work today and be the wrong choice: the moment claims
  # calls into policies directly, the two are one module and the boundary that
  # makes this a modular monolith is gone. Asking costs a millisecond and keeps
  # the seam.
  #
  # The timeout is the part not to skip. A request that waits forever is how
  # one slow module stops the whole application, monolith or not.
  class ClaimsModule
    include AceMQ::AMQP

    # Generous for an in-process hop, and still bounded.
    LOOKUP_TIMEOUT = 5

    attr_reader :settled, :rejected

    def initialize(connection)
      @mq = connection
      @settled = 0
      @rejected = 0
      @requester = Patterns::Requester.new(@mq, to: POLICY_LOOKUP, timeout: LOOKUP_TIMEOUT)
      # Claims listens for issued policies only to know they exist at all; the
      # authoritative answer still comes from the lookup, because a policy can
      # be cancelled after issue and this module deliberately keeps no copy of
      # another module's state.
      @issued = @mq.consume(CLAIMS) { Ack.accept }
    end

    # Assesses a claim against a policy.
    #
    # @return [String] the claim id
    # @raise [RuntimeError] when policies did not answer in time
    def submit(policy_id, amount, _description)
      claim_id = "CLM-#{SecureRandom.uuid[0, 8]}"
      begin
        status = PolicyStatus.from_wire(
          @requester.call(PolicyQuery.new(policy_id: policy_id).to_wire, type: PolicyQuery.type)
        )
      rescue Patterns::RequestTimedOut => e
        # Not an answer, and must not be treated as "no". Refusing a valid claim
        # because a lookup was slow is the failure mode worth being explicit
        # about.
        raise "could not establish whether #{policy_id} is in force, so claim #{claim_id} " \
              "was neither settled nor rejected; it must be retried (#{e.message})"
      end

      if status.in_force
        # A real assessment is a great deal more than this. What matters is that
        # it happened after an authoritative answer rather than after a guess.
        announce(ClaimSettled.new(claim_id: claim_id, policy_id: policy_id, paid: amount),
                 CLAIM_SETTLED, policy_id)
        @settled += 1
      else
        announce(ClaimRejected.new(claim_id: claim_id, policy_id: policy_id,
                                   reason: "no policy in force"),
                 CLAIM_REJECTED, policy_id)
        @rejected += 1
      end
      claim_id
    end

    def close
      @issued.cancel
      @requester.close
    end

    private

    def announce(event, routing_key, policy_id)
      @mq.publish(event.to_wire, to: routing_key, exchange: EXCHANGE, mandatory: true,
                                 type: "Claim", correlation_id: policy_id)
    end
  end
end
