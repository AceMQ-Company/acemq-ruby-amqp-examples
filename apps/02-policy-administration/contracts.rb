# frozen_string_literal: true

require "acemq/amqp"

# What every module in this monolith agrees on, and nothing else.
#
# The same role contracts.rb plays in apps/01, and the reason is sharper here:
# these modules run in one process, so nothing but discipline stops one from
# reaching into another's classes. This file is the only thing they share. Each
# module requires it and none of its siblings -- main.rb is the only file that
# requires more than one module, which is what makes the monolith *modular*
# rather than merely large. A module that only ever received events can be
# lifted into its own process by changing where it connects: there are no call
# sites to find, because there are none.
#
# What is deliberately not here: any module's domain model, any persistence,
# any shared helper. Those are what turn a contracts file into a common-utils
# file, and a common-utils file is how modular monoliths become ordinary ones.
#
# Every name and every field is the one Java's `apps/02-policy-administration`
# uses, camelCase on the wire included.
module Policies
  # One topic exchange for the whole application.
  EXCHANGE = "policy"

  # An event is a Struct, so it cannot carry a field the contract does not name.
  # Members are Ruby's snake_case; the wire is the contract's camelCase, and the
  # two conversions below are the only place that difference exists.
  module Event
    def self.define(*members)
      Struct.new(*members, keyword_init: true) do
        # The envelope type, which is the class name: "PolicyIssued".
        def self.type = name.split("::").last

        def self.from_wire(hash)
          new(**members.to_h { |m| [m, hash.fetch(Event.camel(m))] })
        end

        def to_wire = to_h.transform_keys { |k| Event.camel(k) }
      end
    end

    def self.camel(member) = member.to_s.gsub(/_(\w)/) { Regexp.last_match(1).upcase }
  end

  # ---- events ---------------------------------------------------------------

  # A broker submitted an application. Published by policies, from its outbox.
  ApplicationSubmitted = Event.define(:application_id, :applicant, :product, :sum_assured,
                                      :age_of_applicant)
  # Underwriting reached a decision and priced it.
  ApplicationAccepted = Event.define(:application_id, :applicant, :product, :sum_assured,
                                     :annual_premium)
  # Underwriting refused it, with a reason a human can act on.
  ApplicationDeclined = Event.define(:application_id, :applicant, :reason)
  # A policy exists. Published by policies once underwriting accepted.
  PolicyIssued = Event.define(:policy_id, :application_id, :applicant, :product,
                              :annual_premium)
  # A document belongs to a policy. The document itself is *not* here: this
  # carries a claim check -- the key it was stored under and how big it is --
  # because a medical report scanned at 300 dpi is tens of megabytes and a
  # broker is not a filesystem.
  DocumentStored = Event.define(:policy_id, :document_key, :kind, :bytes)
  # The first premium was taken. Published by billing.
  PremiumCharged = Event.define(:policy_id, :applicant, :amount)
  # Somebody claimed against a policy.
  ClaimSubmitted = Event.define(:claim_id, :policy_id, :amount, :description)
  # The claim was assessed.
  ClaimSettled = Event.define(:claim_id, :policy_id, :paid)
  # It was not, and why.
  ClaimRejected = Event.define(:claim_id, :policy_id, :reason)

  # ---- routing keys ---------------------------------------------------------

  APPLICATION_SUBMITTED = "policy.application.submitted"
  APPLICATION_ACCEPTED = "policy.application.accepted"
  APPLICATION_DECLINED = "policy.application.declined"
  POLICY_ISSUED = "policy.policy.issued"
  DOCUMENT_STORED = "policy.document.stored"
  PREMIUM_CHARGED = "policy.premium.charged"
  CLAIM_SUBMITTED = "policy.claim.submitted"
  CLAIM_SETTLED = "policy.claim.settled"
  CLAIM_REJECTED = "policy.claim.rejected"

  # ---- queues ---------------------------------------------------------------
  #
  # A queue per module, named for the module. Identical to apps/01, and for the
  # identical reason: two modules wanting the same event each get their own
  # copy. That these queues happen to be served by threads in one process is an
  # operational detail, not an architectural one.

  UNDERWRITING = "policy.underwriting"
  POLICIES = "policy.policies"
  BILLING = "policy.billing"
  CLAIMS = "policy.claims"
  # Everything, for the audit trail. Not decoration: writing Java's version
  # without it produced a real failure -- claims and documents published events
  # nothing was bound to. A regulated insurer has this queue whatever else it
  # has, and its absence was a bug in the topology rather than a missing feature.
  AUDIT = "policy.audit"
  # Where claims asks policies whether a policy is in force. Request/reply, not
  # an event.
  POLICY_LOOKUP = "policy.lookup"
  QUEUES = [UNDERWRITING, POLICIES, BILLING, CLAIMS, AUDIT, POLICY_LOOKUP].freeze

  # What claims asks.
  PolicyQuery = Event.define(:policy_id)
  # What policies answers. A record rather than a boolean, so it can grow a reason.
  PolicyStatus = Event.define(:policy_id, :in_force, :annual_premium)

  # The whole application's topology, as one value.
  #
  # Applied once at start-up. In apps/01 every service applied this because
  # none of them could depend on another having started; here there is one
  # process, so it is applied once -- and it is still declared here rather than
  # assembled from each module's fragment, because a module that declares its
  # own queue is a module that can be started against an exchange nobody
  # created.
  #
  # Classic queues, as Java declares them. A queue's type is part of its
  # identity to the broker, so a Ruby module and a Java one sharing these queues
  # have to agree or the second is refused with PRECONDITION_FAILED.
  def self.topology
    AceMQ::AMQP::Topology.new
                         .exchange(EXCHANGE, :topic)
                         # Underwriting acts on submissions.
                         .queue(UNDERWRITING, queue_type: :classic)
                         .binding(UNDERWRITING, EXCHANGE, APPLICATION_SUBMITTED)
                         # Policies issues once underwriting has accepted.
                         .queue(POLICIES, queue_type: :classic)
                         .binding(POLICIES, EXCHANGE, APPLICATION_ACCEPTED)
                         # Billing charges once a policy exists, never before:
                         # charging for a policy that was never issued is a
                         # refund and an apology.
                         .queue(BILLING, queue_type: :classic)
                         .binding(BILLING, EXCHANGE, POLICY_ISSUED)
                         # Claims needs to know which policies exist.
                         .queue(CLAIMS, queue_type: :classic)
                         .binding(CLAIMS, EXCHANGE, POLICY_ISSUED)
                         # Everything, in order. A wildcard also means a new
                         # event type is audited the day it is introduced.
                         .queue(AUDIT, queue_type: :classic)
                         .binding(AUDIT, EXCHANGE, "policy.#")
                         # The queue claims sends questions to. Not bound to the
                         # exchange: a request is addressed to a queue, not
                         # routed to whoever happens to be listening.
                         .queue(POLICY_LOOKUP, queue_type: :classic)
  end
end
