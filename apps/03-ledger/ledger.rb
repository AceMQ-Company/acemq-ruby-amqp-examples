# frozen_string_literal: true

require "securerandom"
require "acemq/amqp"
require "acemq/amqp/patterns"
require_relative "contracts"

module Ledger
  # Balances, computed by reading the journal from the beginning.
  #
  # This is what event sourcing buys: this object holds no state that anybody
  # wrote down. Delete it, restart the process, and it comes back identical,
  # because it is a function of the log and nothing else. +StreamOffset.first+
  # is the whole trick -- a queue cannot do this, because reading it consumes it.
  #
  # == Read to the end, then stop
  #
  # The reader is cancelled once it has caught up, and the writer maintains the
  # balances itself from then on. That is a correctness requirement, not an
  # optimisation: the first Java version kept following the stream *and* applied
  # each entry as it was written, so every entry was counted twice -- once
  # locally and once when it came back round. Keeping only the stream has the
  # opposite problem: a transfer decided against a balance that does not yet
  # include the transfer before it. For a single writer, maintaining its own
  # total after the rebuild is both correct and immediate.
  #
  # == The cost
  #
  # A rebuild is O(history). At a billion entries the answer is a snapshot --
  # "the balance at offset N", plus everything after N -- deliberately not here.
  class Balances
    include AceMQ::AMQP

    # How long without an entry counts as "caught up". Crude and honest about
    # it: the precise way is to read the offset of the last entry before
    # starting and stop there, which needs an offset this example does not
    # otherwise use. Java's number.
    QUIET_PERIOD = 0.4
    REBUILD_LIMIT = 30

    # @return [Balances] caught up with everything already in the journal
    def self.rebuilt_from(connection)
      balances = new
      last_seen = now
      reader = Patterns.read_stream(connection, JOURNAL,
                                    offset: Patterns::StreamOffset.first) do |message|
        balances.apply(EntryPosted.from_wire(message.payload))
        last_seen = now
        Ack.accept
      end
      deadline = now + REBUILD_LIMIT
      while now - last_seen < QUIET_PERIOD
        if now > deadline
          raise "the journal did not stop producing entries within #{REBUILD_LIMIT}s; " \
                "a rebuild cannot finish while somebody is still writing"
        end
        sleep 0.02
      end
      balances
    ensure
      reader&.cancel
    end

    def self.now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    def initialize
      @accounts = Hash.new(0)
      @lock = Mutex.new
    end

    # Applied by the rebuild, then by the writer as it appends -- safe precisely
    # because there is one writer.
    def apply(entry) = @lock.synchronize { @accounts[entry.account] += entry.amount_minor }

    def of(account) = @lock.synchronize { @accounts[account] }
  end

  # The only thing allowed to append to the journal.
  #
  # One writer, deliberately. A ledger's invariant -- every transfer produces two
  # entries that sum to zero -- cannot be enforced by two processes appending
  # independently, and a stream will happily accept an unbalanced pair from each
  # of them. Making the writer singular is what makes the invariant checkable.
  class LedgerModule
    include AceMQ::AMQP

    # How long the journal keeps entries. An hour, because this is an example. A
    # real ledger keeps them for as long as the law says, and this is the
    # setting people get wrong: a stream that expires entries is a ledger that
    # quietly loses the ability to rebuild the balances it claims are derived.
    # If retention is shorter than "forever", the projection is the system of
    # record after all, and nobody wrote that down.
    RETENTION = 3600
    MAX_BYTES = 50 * 1024 * 1024

    attr_reader :posted, :rejected

    def initialize(connection)
      @mq = connection
      @posted = 0
      @rejected = 0
      # Deciding and appending are one step, whichever thread asks: a transfer
      # read off the commands queue and an account opened by the caller must not
      # both decide against the same balance.
      @lock = Mutex.new
      Patterns.declare_stream(@mq, JOURNAL, max_age: RETENTION, max_bytes: MAX_BYTES)
      # The writer's own view of the balances, rebuilt from the journal on
      # start-up. Not a cache of somebody else's state: it is derived here, from
      # the log, for the one decision this module has to make.
      @balances = Balances.rebuilt_from(@mq)
      @commands = @mq.consume(COMMANDS) do |message|
        apply(Transfer.from_wire(message.payload))
        Ack.accept
      end
    end

    # Opens an account with money in it, which every ledger needs a way to do.
    def fund(account, amount_minor)
      @lock.synchronize do
        post(EntryPosted.new(entry_id: entry_id, transfer_id: "OPENING-#{account}",
                             account: account, amount_minor: amount_minor,
                             description: "opening balance"))
      end
    end

    # This module's own view, derived from the log.
    def balance_of(account) = @balances.of(account)

    def close = @commands.cancel

    private

    def apply(transfer)
      @lock.synchronize do
        available = @balances.of(transfer.from)
        if transfer.amount_minor <= 0
          next reject(transfer, "a transfer must be for a positive amount")
        end
        if available < transfer.amount_minor
          # Refused, and the refusal is recorded. A ledger that silently drops
          # what it will not do cannot explain itself later.
          next reject(transfer, "insufficient funds: #{transfer.from} holds #{available}")
        end

        # Two entries, one transfer, summing to zero, appended one after the
        # other by the only writer there is. A real ledger appends them as one
        # record so a crash between them is impossible; that is the honest
        # limitation of doing it this way.
        post(EntryPosted.new(entry_id: entry_id, transfer_id: transfer.transfer_id,
                             account: transfer.from, amount_minor: -transfer.amount_minor,
                             description: transfer.description))
        post(EntryPosted.new(entry_id: entry_id, transfer_id: transfer.transfer_id,
                             account: transfer.to, amount_minor: transfer.amount_minor,
                             description: transfer.description))
      end
    end

    # Published straight at the stream by name: a stream is addressed as a
    # queue, so the default exchange and the stream's name is the whole of it.
    # Mandatory, as Java's publishes are by default: an entry the broker could
    # not route is an entry the log does not have.
    def post(entry)
      @mq.publish(entry.to_wire, to: JOURNAL, mandatory: true, id: entry.entry_id,
                                 type: EntryPosted.type, correlation_id: entry.transfer_id)
      @balances.apply(entry)
      @posted += 1
    end

    def reject(transfer, reason)
      @rejected += 1
      @mq.publish(TransferRejected.new(transfer_id: transfer.transfer_id, from: transfer.from,
                                       to: transfer.to, amount_minor: transfer.amount_minor,
                                       reason: reason).to_wire,
                  to: TRANSFER_REJECTED, exchange: EXCHANGE, mandatory: true,
                  type: TransferRejected.type, correlation_id: transfer.transfer_id)
    end

    def entry_id = "E-#{SecureRandom.uuid}"
  end
end
