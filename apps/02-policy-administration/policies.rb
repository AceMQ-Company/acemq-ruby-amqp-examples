# frozen_string_literal: true

require "securerandom"
require "acemq/amqp"
require "acemq/amqp/patterns"
require_relative "contracts"

module Policies
  # Applications and policies: the module that owns the records everything else
  # refers to.
  #
  # Two things here are worth the reading.
  #
  # **The outbox is still necessary.** This is a monolith with one database, so
  # the usual argument for an outbox -- two services, two datastores -- does not
  # apply. It applies anyway, because the two systems that must agree are *this
  # database* and *the broker*, and no transaction spans both. Saving an
  # application and announcing it are still two writes, and a crash between them
  # still loses one. A monolith removes the distributed transaction between
  # modules; it does not remove the one between a module and its broker.
  #
  # **Claims asks this module a question rather than reading its tables.** The
  # module answers on a queue. In one process a direct call would obviously
  # work, which is exactly why the discipline matters: the moment claims calls a
  # method here, the two modules are one module.
  class PolicyModule
    include AceMQ::AMQP

    attr_reader :issued

    # @param connection [Connection] the application's one connection
    # @param database [#call] opens a handle on the application's one database;
    #   the nearest thing Ruby has to Java's DataSource
    def initialize(connection, database:)
      @mq = connection
      @issued = 0
      # One handle for this module's own reads and writes, used from the
      # caller's thread (submit), the consumer's (issue) and the responder's
      # (status_of). SQLite allows one transaction per handle, so the lock is
      # what stops an issue landing in the middle of a submit's transaction.
      @db = database.call
      @lock = Mutex.new
      create_schema

      # The relay gets a handle of its own, because it runs on its own thread
      # and must never find itself inside somebody's transaction. A record
      # written on a different handle would be a record in a different
      # transaction, which is the bug the outbox exists to prevent -- so the
      # write side always passes @db.
      @outbox = Patterns::SQLOutboxStore.new(connection: @db,
                                             relay: database.call,
                                             lease: 30)
      @outbox.create_schema
      @relay = Patterns::OutboxRelay.new(
        @mq, @outbox, interval: 0.2, batch: 20,
                      on_error: ->(e) { warn "policies: outbox relay: #{e.message}" }
      ).start

      @accepted = @mq.consume(POLICIES) do |message|
        issue(ApplicationAccepted.from_wire(message.payload))
        Ack.accept
      end
      @lookups = Patterns.serve(@mq, POLICY_LOOKUP) do |message|
        status_of(PolicyQuery.from_wire(message.payload).policy_id).to_wire
      end
    end

    # Takes an application, and announces it, in one transaction.
    #
    # @return [String] the application id
    def submit(applicant, product, sum_assured, age)
      application_id = "APP-#{SecureRandom.uuid[0, 8]}"
      event = ApplicationSubmitted.new(application_id: application_id, applicant: applicant,
                                       product: product, sum_assured: sum_assured,
                                       age_of_applicant: age)
      @lock.synchronize do
        # One commit decides both. Either the application exists and the event
        # is queued, or neither happened.
        @db.transaction do
          @db.execute("INSERT INTO applications VALUES (?, ?, ?, ?, ?)",
                      [application_id, applicant, product, sum_assured, age])
          @outbox.add(Patterns.record(@mq, event.to_wire,
                                      to: APPLICATION_SUBMITTED, exchange: EXCHANGE,
                                      correlation_id: application_id,
                                      type: ApplicationSubmitted.type),
                      connection: @db)
        end
      end
      application_id
    end

    # The premium recorded for a policy, when it exists.
    def premium_of(policy_id)
      status = status_of(policy_id)
      status.in_force ? status.annual_premium : nil
    end

    def close
      @lookups.cancel
      @accepted.cancel
      @relay.close
    end

    private

    # Underwriting said yes, so the policy exists.
    def issue(accepted)
      policy_id = "POL-#{SecureRandom.uuid[0, 8]}"
      event = PolicyIssued.new(policy_id: policy_id, application_id: accepted.application_id,
                               applicant: accepted.applicant, product: accepted.product,
                               annual_premium: accepted.annual_premium)
      @lock.synchronize do
        @db.transaction do
          @db.execute("INSERT INTO policies VALUES (?, ?, ?, ?, ?)",
                      [policy_id, accepted.application_id, accepted.applicant,
                       accepted.product, accepted.annual_premium])
          @outbox.add(Patterns.record(@mq, event.to_wire,
                                      to: POLICY_ISSUED, exchange: EXCHANGE,
                                      correlation_id: accepted.application_id,
                                      type: PolicyIssued.type),
                      connection: @db)
        end
        @issued += 1
      end
    end

    # Answers the question claims asks, without claims touching this module's
    # tables.
    def status_of(policy_id)
      premium = @lock.synchronize do
        @db.get_first_value("SELECT premium FROM policies WHERE id = ?", [policy_id])
      end
      PolicyStatus.new(policy_id: policy_id, in_force: !premium.nil?,
                       annual_premium: premium || 0)
    end

    def create_schema
      @db.execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS applications (
          id TEXT PRIMARY KEY, applicant TEXT, product TEXT, sum_assured INTEGER, age INTEGER)
      SQL
      @db.execute(<<~SQL)
        CREATE TABLE IF NOT EXISTS policies (
          id TEXT PRIMARY KEY, application_id TEXT, applicant TEXT, product TEXT, premium INTEGER)
      SQL
    end
  end
end
