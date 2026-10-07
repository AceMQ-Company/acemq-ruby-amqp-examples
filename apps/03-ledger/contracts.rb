# frozen_string_literal: true

require "acemq/amqp"

# The events a ledger is made of.
#
# In apps/01 and apps/02 the events describe what happened *to* the system of
# record. Here they *are* the system of record. There is no balances table that
# events update; a balance is what you get by adding up entries, and it can be
# deleted and recomputed without losing anything, because nothing was ever
# stored that the log does not contain.
#
# That is the whole claim, and it has one consequence worth stating before the
# code: **an entry is never changed and never deleted.** Money moved wrongly is
# corrected by posting the opposite entry, exactly as a paper ledger does, and
# both entries stay.
#
# Every name and every field is the one Java's `apps/03-ledger` uses, camelCase
# on the wire included.
module Ledger
  # The stream every entry is appended to.
  #
  # A stream rather than a queue, and the difference is the point. A queue is
  # emptied by being read; a stream is not. Ten readers can each read all of
  # history at their own pace, a new projection can start from offset zero next
  # year, and nothing anybody reads removes anything for anybody else.
  JOURNAL = "ledger.journal"
  # Where transfer commands arrive. An ordinary queue: a command is handled once.
  COMMANDS = "ledger.commands"
  # Where refusals are announced, for whoever wants to be told rather than read.
  REJECTIONS = "ledger.rejections"
  # Where the ledger announces what it accepted, for anything not a projection.
  EXCHANGE = "ledger"
  ENTRY_POSTED = "ledger.entry.posted"
  TRANSFER_REQUESTED = "ledger.transfer.requested"
  TRANSFER_REJECTED = "ledger.transfer.rejected"

  # An event is a Struct, so it cannot carry a field the contract does not name.
  # Members are Ruby's snake_case; the wire is the contract's camelCase.
  module Event
    def self.define(*members)
      Struct.new(*members, keyword_init: true) do
        def self.type = name.split("::").last

        def self.from_wire(hash)
          new(**members.to_h { |m| [m, hash.fetch(Event.camel(m))] })
        end

        def to_wire = to_h.transform_keys { |k| Event.camel(k) }
      end
    end

    def self.camel(member) = member.to_s.gsub(/_(\w)/) { Regexp.last_match(1).upcase }
  end

  # ---- the log --------------------------------------------------------------

  # One side of one movement of money.
  #
  # Signed rather than a debit/credit flag: a sum over a column is then simply a
  # sum. Whole minor units -- pennies, cents -- in an Integer, because a ledger in
  # Float is a ledger that disagrees with itself after enough additions.
  #
  # +entry_id+ is unique and the idempotency key; +transfer_id+ is the movement
  # this is one half of; +amount_minor+ is positive to credit, negative to debit.
  EntryPosted = Event.define(:entry_id, :transfer_id, :account, :amount_minor, :description)
  # A transfer that was refused, with the reason kept beside the ones that were not.
  TransferRejected = Event.define(:transfer_id, :from, :to, :amount_minor, :reason)

  # ---- commands -------------------------------------------------------------

  # Move money between two accounts. Not an event: a request, which may be refused.
  Transfer = Event.define(:transfer_id, :from, :to, :amount_minor, :description)

  # The whole application's topology. The journal is declared by its only
  # writer, with its retention, rather than here: a stream's retention is part of
  # what it is, and belongs to whoever answers for keeping it.
  #
  # Classic queues, as Java declares them, so a Ruby and a Java module sharing
  # them agree on what they are.
  def self.topology
    AceMQ::AMQP::Topology.new
                         .exchange(EXCHANGE, :topic)
                         # Commands: an ordinary queue, because a transfer must
                         # be applied once.
                         .queue(COMMANDS, queue_type: :classic)
                         .binding(COMMANDS, EXCHANGE, TRANSFER_REQUESTED)
                         # Rejections are announced so somebody can act on them.
                         .queue(REJECTIONS, queue_type: :classic)
                         .binding(REJECTIONS, EXCHANGE, TRANSFER_REJECTED)
  end
end
