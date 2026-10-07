# frozen_string_literal: true

# The ledger, against a real broker with a real stream.
#
# Java's five system tests, with the same checks, each against a freshly
# started ledger -- which rebuilds its balances from the journal every time,
# so each scenario after the first starts by reading back everything the ones
# before it wrote. The scenarios that matter are the last three: a projection
# rebuilt from nothing that agrees with the writer, one started later that still
# sees all of history, and two that do not compete. Those are the claims event
# sourcing makes, and they are either true or the architecture is a story.
#
# Afterwards, one more writer is started from nothing but the journal, and has
# to arrive at every balance the five scenarios left behind.

require "acemq/amqp"
require_relative "contracts"
require_relative "ledger"
require_relative "projections"
require_relative "transfers"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

def check(claim, what)
  abort "FAILED: #{what}" unless claim
  puts "  ok  #{what}"
end

def wait_for(what, seconds: 90)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
  until yield
    if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
      abort "FAILED: the ledger did not reach the expected state in time: #{what}"
    end
    sleep 0.05
  end
end

# A journal an earlier run left behind would be rebuilt into this run's
# balances, so it starts empty -- the one time anything is ever deleted from it,
# and only because this is a demonstration that must be repeatable.
Connection.open(URL).tap do |mq|
  [Ledger::JOURNAL, Ledger::COMMANDS, Ledger::REJECTIONS].each do |name|
    [name, Naming.dead_letter_queue(name), Naming.parked_queue(name)].each do |queue|
      mq.delete_queue(queue) if mq.queue_exists?(queue)
    end
  end
  mq.close
end

# Starts the ledger and the gateway on a connection of their own, runs one
# scenario, and stops them, whatever happened.
def with_the_ledger(title)
  puts title
  mq = Connection.open(URL, origin: "ledger")
  Ledger.topology.apply(mq)
  ledger = Ledger::LedgerModule.new(mq)
  transfers = Ledger::TransferGateway.new(mq)
  yield ledger, transfers, mq
ensure
  [transfers, ledger].compact.each(&:close)
  mq&.close(timeout: 10)
  puts
end

with_the_ledger("a transfer posts two entries that sum to zero") do |ledger, transfers|
  ledger.fund("alice", 10_000)
  wait_for("alice funded") { ledger.balance_of("alice") == 10_000 }
  transfers.request("alice", "bob", 2_500, "rent")
  wait_for("bob paid") { ledger.balance_of("bob") == 2_500 }
  check ledger.balance_of("alice") == 7_500, "alice holds 7,500"
  # The invariant: money is neither created nor destroyed by a transfer.
  check ledger.balance_of("alice") + ledger.balance_of("bob") == 10_000,
        "and the two still sum to 10,000"
end

with_the_ledger("a transfer that would overdraw is refused, " \
                "and the refusal is recorded") do |ledger, transfers|
  ledger.fund("carol", 1_000)
  wait_for("carol funded") { ledger.balance_of("carol") == 1_000 }
  transfers.request("carol", "dave", 5_000, "optimistic")
  wait_for("the refusal") { !transfers.refused.empty? }
  reason = transfers.refused.first.reason
  check reason.include?("insufficient funds"), "refused: #{reason}"
  # Nothing was posted. A ledger that half-applies a refused transfer is worse
  # than one that refuses loudly.
  check ledger.balance_of("carol") == 1_000, "carol still holds 1,000"
  check ledger.balance_of("dave").zero?, "dave holds nothing"
end

with_the_ledger("a projection built from offset zero " \
                "agrees with the writer") do |ledger, transfers, mq|
  ledger.fund("erin", 20_000)
  wait_for("erin funded") { ledger.balance_of("erin") == 20_000 }
  transfers.request("erin", "frank", 3_000, "invoice 1")
  transfers.request("erin", "frank", 4_000, "invoice 2")
  wait_for("frank paid twice") { ledger.balance_of("frank") == 7_000 }
  # A reader that has never seen a message before, starting from the beginning
  # of time. It stores nothing the log does not contain, and must reach the
  # same answer.
  statements = Ledger::StatementProjection.new(mq, from_first: true)
  begin
    wait_for("the projection caught up") { statements.balance_of("frank") == 7_000 }
    check statements.balance_of("erin") == ledger.balance_of("erin"),
          "erin: the projection and the writer agree (#{statements.balance_of("erin")})"
    check statements.balance_of("frank") == ledger.balance_of("frank"),
          "frank: the projection and the writer agree (#{statements.balance_of("frank")})"
    # And it has the detail the balance does not: the opening balance and two
    # debits against erin.
    check statements.statement_of("erin").size == 3, "erin's statement has three entries"
    check statements.statement_of("frank").map(&:description) == ["invoice 1", "invoice 2"],
          "frank's reads invoice 1, invoice 2"
  ensure
    statements.close
  end
end

with_the_ledger("a projection added later still gets all of history") do |ledger, transfers, mq|
  ledger.fund("grace", 5_000)
  transfers.request("grace", "heidi", 1_000, "before the projection existed")
  wait_for("heidi paid") { ledger.balance_of("heidi") == 1_000 }
  # Started now, after the entries were written. On a queue there would be
  # nothing left to read -- the ledger's own reader consumed it. A stream is not
  # emptied by reading, so this gets everything, and so would one written next
  # year.
  late = Ledger::StatementProjection.new(mq, from_first: true)
  begin
    wait_for("the late projection caught up") { late.balance_of("heidi") == 1_000 }
    check late.statement_of("heidi").size == 1, "heidi's statement has one entry"
    check late.statement_of("heidi").first.description == "before the projection existed",
          "written before the projection existed"
  ensure
    late.close
  end
end

with_the_ledger("two readers of the same stream " \
                "do not compete for entries") do |ledger, transfers, mq|
  ledger.fund("ivan", 8_000)
  transfers.request("ivan", "judy", 2_000, "shared")
  wait_for("judy paid") { ledger.balance_of("judy") == 2_000 }
  first = Ledger::StatementProjection.new(mq, from_first: true)
  second = Ledger::StatementProjection.new(mq, from_first: true)
  begin
    wait_for("both caught up") do
      first.balance_of("judy") == 2_000 && second.balance_of("judy") == 2_000
    end
    # Both saw the same entry. On a queue exactly one of them would have, which
    # is the property that makes a queue wrong for a ledger and right for a
    # command.
    check first.statement_of("judy").size == 1, "the first reader saw judy's entry"
    check second.statement_of("judy").size == 1, "and so did the second"
  ensure
    first.close
    second.close
  end
end

# Every balance the scenarios left behind, and the entries they appended: one
# per account opened, two per transfer that went through, none for the refusal.
EXPECTED = {
  "alice" => 7_500, "bob" => 2_500, "carol" => 1_000, "dave" => 0,
  "erin" => 13_000, "frank" => 7_000, "grace" => 4_000, "heidi" => 1_000,
  "ivan" => 6_000, "judy" => 2_000
}.freeze
ENTRIES = 5 + (2 * 5)

# The claim the whole application rests on, made directly: a writer started
# with nothing but the journal arrives at every balance, and the journal holds
# exactly the entries that were posted -- one fewer is an entry lost, one more
# an entry appended twice.
with_the_ledger("a writer started from nothing " \
                "but the journal has every balance") do |ledger, _, mq|
  EXPECTED.each do |account, balance|
    check ledger.balance_of(account) == balance, "#{account} holds #{balance}"
  end
  everything = Ledger::StatementProjection.new(mq, from_first: true)
  begin
    wait_for("the whole journal") { everything.entries >= ENTRIES }
    sleep 1
    check everything.entries == ENTRIES,
          "the journal holds #{everything.entries} of #{ENTRIES} entries"
    put_in = 10_000 + 1_000 + 20_000 + 5_000 + 8_000
    check everything.accounts.sum { |a| everything.balance_of(a) } == put_in,
          "and sums to the money that was put in, no more and no less"
  ensure
    everything.close
  end
  Connection.open(URL).tap do |admin|
    [Ledger::COMMANDS, Ledger::REJECTIONS].each do |queue|
      dlq = Naming.dead_letter_queue(queue)
      held = admin.queue_exists?(dlq) ? admin.message_count(dlq) : 0
      check held.zero?, "#{dlq} holds #{held}"
    end
    admin.close
  end
end
