# frozen_string_literal: true

require "acemq/amqp"
require "acemq/amqp/patterns"
require_relative "contracts"

module Policies
  # Deciding whether to accept an application, and at what price.
  #
  # Underwriting is the one part of this application that is genuinely a
  # *sequence*: check the applicant against the register, price the risk, then
  # decide. Each stage can fail for its own reasons and is slow for its own
  # reasons, which is what a {AceMQ::AMQP::Patterns::Pipeline} is for -- a queue
  # per stage, so a slow stage shows up as a deep queue you can point at, and
  # can be retried and scaled without touching the others.
  #
  # Written as one consumer doing three things in order, all of that disappears:
  # one queue, one failure mode, and one number that says "underwriting is slow".
  class UnderwritingModule
    include AceMQ::AMQP

    # Applications above this are a human's decision, not a rule's.
    REFERRAL_THRESHOLD = 500_000

    # The step names are the routing keys and the queue suffixes, so they stay
    # short and stable: renaming one strands whatever is in flight against the
    # old name. Java's names exactly, so the queues are the same queues.
    PIPELINE = Patterns::Pipeline.new("underwriting", %w[register price decide])

    # What each step does, in words. Java's builder carries these as
    # +describedAs+; Ruby's Pipeline is a list of names, so they live here and
    # are printed in the same start-up line Java logs, which is where somebody
    # unfamiliar with this system finds out what the stages actually do.
    DESCRIPTIONS = {
      "register" => "look the applicant up on the shared industry register",
      "price" => "apply the rating table for the product and the applicant's age",
      "decide" => "accept, or refer anything a rule should not be deciding"
    }.freeze

    attr_reader :accepted, :declined

    def initialize(connection, log: $stdout)
      @mq = connection
      @accepted = 0
      @declined = 0
      @lock = Mutex.new

      @mq.apply(PIPELINE.topology)
      log.puts "      pipeline #{PIPELINE.name} declared with #{PIPELINE.steps.size} steps: " +
               PIPELINE.steps.map { |s| "#{s} (#{DESCRIPTIONS.fetch(s)})" }.join(" | ")

      @steps = [
        # The register is somebody else's service and is periodically
        # unavailable, which is a wait rather than a decline -- so this step,
        # and only this one, has a retry ladder.
        step("register", RetryPolicy.exponential(4, 0.2, 5)) { |m| check_register(m.payload) },
        step("price") { |m| price(m.payload) },
        step("decide") { |m| decide(m.payload) }
      ]

      # The pipeline is fed from the module's own queue rather than bound to the
      # exchange itself: the pipeline owns its stages' queues, and what enters it
      # is this module's decision.
      @submissions = @mq.consume(UNDERWRITING) do |message|
        PIPELINE.start(@mq, message.payload,
                       correlation_id: message.envelope.correlation_id,
                       type: ApplicationSubmitted.type)
        Ack.accept
      end
    end

    def close
      @submissions.cancel
      @steps.each(&:cancel)
    end

    private

    def step(name, retry_policy = nil, &)
      @mq.consume(PIPELINE.queue_for(name), retry_policy: retry_policy,
                  &PIPELINE.follow(@mq, &))
    end

    # What each stage hands the next, as Java's records serialise: +Checked+ is
    # the application and whether the register knows the applicant, +Priced+
    # the application, the premium and whether to refer it.
    def check_register(application)
      # A real one calls an industry service. The interesting part for this
      # example is that it is the stage most likely to be slow, and it has its
      # own queue to prove it.
      { "application" => application,
        "knownToRegister" => application.fetch("applicant").downcase.include?("known") }
    end

    def price(checked)
      application = checked.fetch("application")
      sum_assured = application.fetch("sumAssured")
      # A rating table, compressed to one line. Older applicants and larger sums
      # cost more. Integer division, as Java's int arithmetic does it.
      base = sum_assured / 1000
      age_loading = [0, application.fetch("ageOfApplicant") - 30].max * 2
      register_loading = checked.fetch("knownToRegister") ? base / 2 : 0
      { "application" => application,
        "annualPremium" => base + age_loading + register_loading,
        "refer" => sum_assured > REFERRAL_THRESHOLD }
    end

    # The last step. It returns what it decided on rather than nil, because a
    # step that returns nil *ends the run early* -- a filter's answer -- and the
    # library counts that apart from a run that finished.
    def decide(priced)
      application = ApplicationSubmitted.from_wire(priced.fetch("application"))
      if priced.fetch("refer")
        # Declined rather than parked, because "a human must look at this" is a
        # real outcome of underwriting and not a failure of it. Modelling it as
        # an error would put it in a dead-letter queue, where it would look like
        # something broke.
        announce(ApplicationDeclined.new(
                   application_id: application.application_id, applicant: application.applicant,
                   reason: "sum assured of #{application.sum_assured} " \
                           "is above the automatic limit"
                 ), APPLICATION_DECLINED)
        @lock.synchronize { @declined += 1 }
      else
        announce(ApplicationAccepted.new(
                   application_id: application.application_id, applicant: application.applicant,
                   product: application.product, sum_assured: application.sum_assured,
                   annual_premium: priced.fetch("annualPremium")
                 ), APPLICATION_ACCEPTED)
        @lock.synchronize { @accepted += 1 }
      end
      priced
    end

    # +mandatory: true+ because Java's publishers are mandatory by default and
    # Ruby's are not: without it an event nothing is bound to is confirmed and
    # dropped, which is exactly the bug Java's version of this application found
    # in its own topology. See the README.
    def announce(event, routing_key)
      @mq.publish(event.to_wire, to: routing_key, exchange: EXCHANGE, mandatory: true,
                                 type: "UnderwritingDecision",
                                 correlation_id: event.application_id)
    end
  end
end
