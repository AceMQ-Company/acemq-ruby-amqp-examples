# frozen_string_literal: true

require "acemq/amqp"
require "acemq/amqp/patterns"
require_relative "contracts"

module Ledger
  # A statement per account, built by reading the journal.
  #
  # This module exists to make one claim checkable: **a projection is
  # disposable**. It stores nothing the log does not contain, it is built by
  # reading from offset zero, and throwing it away costs nothing but the time to
  # read the log again.
  #
  # It also proves the log is genuinely shared. The ledger reads the same stream
  # from the same offset for its own purposes, and neither reader affects the
  # other -- no competing consumption, no "who got the message". That is the
  # property a queue does not have, and the reason a ledger wants a stream.
  #
  # A fraud model, a tax report, a daily-balance chart: each is a new reader
  # from offset zero, added without touching the writer, without a migration,
  # and with full history from the day it starts.
  class StatementProjection
    include AceMQ::AMQP

    # @param from_first [Boolean] all of history, or only what arrives from now
    def initialize(connection, from_first:)
      @statements = Hash.new { |h, k| h[k] = [] }
      @lock = Mutex.new
      offset = from_first ? Patterns::StreamOffset.first : Patterns::StreamOffset.next
      @reader = Patterns.read_stream(connection, JOURNAL, offset: offset) do |message|
        entry = EntryPosted.from_wire(message.payload)
        @lock.synchronize { @statements[entry.account] << entry }
        Ack.accept
      end
    end

    # The entries seen for an account, oldest first.
    def statement_of(account) = @lock.synchronize { @statements.fetch(account, []).dup }

    # The sum of the entries, which is what a balance is.
    def balance_of(account) = statement_of(account).sum(&:amount_minor)

    # Every account this projection has seen an entry for.
    def accounts = @lock.synchronize { @statements.keys }

    # Entries read.
    def entries = @lock.synchronize { @statements.values.sum(&:size) }

    def close = @reader.cancel
  end
end
