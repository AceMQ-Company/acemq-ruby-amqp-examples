# frozen_string_literal: true

# The whole application, one process, one connection, one database.
#
# Which is what it is in production too -- that is the point of a monolith. The
# difference from apps/01 is not the number of processes but where the
# boundaries are: here they are files that require nothing but contracts.rb,
# and a broker. This is the only file that requires more than one module.
#
# Five scenarios, each into a freshly started application with a fresh
# database -- Java's five system tests, with the same checks. Every claim is
# checked, and the first that does not hold ends the run non-zero.

require "tmpdir"
require "securerandom"
require "sqlite3"
require "acemq/amqp"
require_relative "contracts"
require_relative "policies"
require_relative "underwriting"
require_relative "documents"
require_relative "billing"
require_relative "claims"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

def check(claim, what)
  abort "FAILED: #{what}" unless claim
  puts "  ok  #{what}"
end

# Polls rather than sleeps, and waits for the final state rather than an
# intermediate one.
def wait_for(what, seconds: 90)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
  until yield
    if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      abort "FAILED: the application did not reach the expected state in time: #{what}"
    end
    sleep 0.05
  end
end

STAGES = Policies::UnderwritingModule::PIPELINE.steps.map do |step|
  Policies::UnderwritingModule::PIPELINE.queue_for(step)
end
ALL_QUEUES = Policies::QUEUES + STAGES

# Whatever an earlier run left behind would turn up in this run's counters, so
# the queues start empty. They are declared again on start-up.
Connection.open(URL).tap do |mq|
  ALL_QUEUES.each do |queue|
    [queue, Naming.dead_letter_queue(queue), Naming.parked_queue(queue)].each do |name|
      mq.delete_queue(name) if mq.queue_exists?(name)
    end
  end
  mq.close
end

App = Struct.new(:mq, :policies, :underwriting, :documents, :billing, :claims, :telemetry,
                 keyword_init: true)

# Starts the application, runs one scenario, and stops it, whatever happened.
def with_the_application(title)
  puts title
  Dir.mktmpdir("policy") do |dir|
    path = File.join(dir, "policy.db")
    # One database, several modules -- the monolith's actual advantage. The
    # outbox still has to exist, because the broker is not in its transaction.
    # Each module is handed a way to open a handle, Ruby's nearest thing to a
    # DataSource, and opens as many as it has threads that use one.
    database = lambda do
      SQLite3::Database.new(path).tap do |db|
        # WAL so a reader does not wait for a writer, and a busy handler that
        # releases Ruby's lock while it waits. Plain +busy_timeout+ holds it,
        # so the thread holding the database lock cannot run to release it.
        db.busy_handler_timeout = 5_000
        db.execute("PRAGMA journal_mode = WAL")
      end
    end

    app = App.new(telemetry: Telemetry::Registry.new)
    # One connection for the whole application, because it is one application.
    # In apps/01 each service had its own; sharing one here is correct, and is
    # the only thing that genuinely differs at the transport level.
    app.mq = Connection.open(URL, origin: "policy-administration", telemetry: app.telemetry)
    Policies.topology.apply(app.mq)
    app.policies = Policies::PolicyModule.new(app.mq, database: database)
    app.underwriting = Policies::UnderwritingModule.new(app.mq)
    app.documents = Policies::DocumentModule.new(app.mq)
    app.billing = Policies::BillingModule.new(app.mq, database: database)
    app.claims = Policies::ClaimsModule.new(app.mq)
    yield app
  ensure
    [app.claims, app.billing, app.underwriting, app.policies].compact.each(&:close)
    app.mq&.close(timeout: 10)
  end
  puts
end

# How many runs the underwriting pipeline finished, read off the library's own
# telemetry rather than this application's counters.
def pipeline_runs(app, outcome)
  app.telemetry[Telemetry::PIPELINE_RUN_TOTAL, pipeline: "underwriting", step: "decide",
                                               outcome: outcome]
end

# What the audit queue must hold at the end: every event published to the
# exchange, once. Counted per scenario as it goes.
audited = 0

with_the_application("an ordinary application becomes a policy, " \
                     "and the premium is taken once") do |a|
  a.policies.submit("A. Applicant", "TERM-LIFE", 100_000, 40)
  # Submitted -> underwritten -> issued -> charged, with no module calling another.
  wait_for("charged") { a.billing.charges.size == 1 }
  check a.underwriting.accepted == 1, "underwriting accepted it"
  check a.policies.issued == 1, "policies issued it"
  check a.billing.charges.size == 1, "billing charged once"
  # 100 base + 20 age loading, from the rating table in the pricing stage.
  premium = a.policies.premium_of(a.billing.charges.first)
  check premium == 120, "the premium is 120 (#{premium})"
  check pipeline_runs(a, Telemetry::Outcome::COMPLETED) == 1,
        "the pipeline finished one run, all three steps"
  audited += 4 # submitted, accepted, issued, charged
end

with_the_application("an application above the automatic limit is referred, " \
                     "and never becomes a policy") do |a|
  a.policies.submit("B. Applicant", "TERM-LIFE", 750_000, 35)
  wait_for("declined") { a.underwriting.declined == 1 }
  # Nothing downstream ran. Billing charging for a referred application would
  # be the expensive version of this bug, which is why it is bound to
  # PolicyIssued and not to the application.
  sleep 0.5
  check a.policies.issued.zero?, "no policy was issued"
  check a.billing.charges.empty?, "nothing was charged"
  audited += 2 # submitted, declined
end

with_the_application("a claim is assessed against an answer from policies, " \
                     "not against a local copy") do |a|
  a.policies.submit("C. Applicant", "TERM-LIFE", 50_000, 30)
  wait_for("charged") { a.billing.charges.size == 1 }
  policy_id = a.billing.charges.first
  a.claims.submit(policy_id, 5_000, "windscreen")
  # A policy nobody issued. The lookup is what makes this answerable at all.
  a.claims.submit("POL-does-not-exist", 5_000, "windscreen")
  check a.claims.settled == 1, "the claim on the issued policy was settled"
  check a.claims.rejected == 1, "the claim on a policy nobody issued was rejected"
  audited += 6 # submitted, accepted, issued, charged, settled, rejected
end

with_the_application("a large document travels as a claim check, not as a message") do |a|
  a.policies.submit("D. Applicant", "TERM-LIFE", 60_000, 45)
  wait_for("charged") { a.billing.charges.size == 1 }
  policy_id = a.billing.charges.first
  # Four megabytes, which is a small scan and a large message.
  scan = "\0".b * (4 * 1024 * 1024)
  key = a.documents.store(policy_id, "medical-report", scan)
  check !a.documents.fetch(key).nil?, "the store has it"
  check a.documents.fetch(key).bytesize == scan.bytesize, "all four megabytes of it"
  # What crossed the broker is the key and the size; the four megabytes never
  # went near a queue.
  check key.include?(policy_id) && key.include?("medical-report"),
        "the key names the policy and the kind (#{key})"
  audited += 5 # submitted, accepted, issued, charged, stored
end

with_the_application("three copies of one event charge once; " \
                     "a genuinely different event still charges") do |a|
  a.policies.submit("E. Applicant", "TERM-LIFE", 80_000, 50)
  wait_for("charged") { a.billing.charges.size == 1 }
  policy_id = a.billing.charges.first
  premium = a.policies.premium_of(policy_id)
  # Three copies carrying one message id: what a redelivery looks like from the
  # consumer's side, and what the idempotency store exists to absorb.
  message_id = "redelivery-#{SecureRandom.uuid}"
  copy = Policies::PolicyIssued.new(policy_id: policy_id, application_id: "APP-x",
                                    applicant: "E. Applicant", product: "TERM-LIFE",
                                    annual_premium: premium)
  3.times do
    a.mq.publish(copy.to_wire, to: Policies::POLICY_ISSUED, exchange: Policies::EXCHANGE,
                               mandatory: true, id: message_id, type: Policies::PolicyIssued.type)
  end
  # Two, not one -- and the difference is the whole point. The store
  # deduplicates by message id, so the three copies are one charge. They are not
  # the *same* message as the original issue, which had an id of its own, so
  # suppressing that too would mean the store had stopped distinguishing "sent
  # twice" from "happened twice".
  wait_for("the second charge") { a.billing.charges.size == 2 }
  sleep 2
  check a.billing.charges.size == 2, "three copies charged once, beside the original"
  audited += 8 # submitted, accepted, issued, charged; three copies; one charge
end

# Nothing was given up on, and nothing was lost or doubled on its way. A message
# sent to a dead-letter queue fails none of the checks above, so the broker is
# asked directly -- and the audit queue, bound to everything, must hold exactly
# the events the five scenarios published: one fewer is a lost message, one more
# a duplicate the relay or a retry introduced.
puts "afterwards"
Connection.open(URL).tap do |mq|
  given_up = ALL_QUEUES.flat_map { |q| [Naming.dead_letter_queue(q), Naming.parked_queue(q)] }
  given_up.each do |name|
    held = mq.queue_exists?(name) ? mq.message_count(name) : 0
    check held.zero?, "#{name} holds #{held}"
  end
  wait_for("the audit trail") { mq.message_count(Policies::AUDIT) >= audited }
  sleep 1
  held = mq.message_count(Policies::AUDIT)
  check held == audited,
        "#{Policies::AUDIT} holds every event exactly once (#{held} of #{audited})"
  mq.close
end
