# frozen_string_literal: true

# The message written down in the same transaction as the work, and a relay
# publishing what was committed.
#
# A service that writes to a database and then publishes has two things that
# can fail independently. Crash between them and the work is committed with
# nobody told; publish first and then fail to commit, and the world has been
# told about something that did not happen. Writing the message into the same
# transaction removes the gap — both commit or neither does — and the relay
# publishes afterwards.
#
# The store below is written out in full rather than taken from the library on
# purpose. `Patterns::InMemoryOutboxStore` exists, and it works, but it has its
# own memory: `add` cannot join a transaction it knows nothing about, so it
# would demonstrate the shape of the pattern while quietly not having the
# property the pattern is for. The real one is `Patterns::SQLOutboxStore`,
# which takes your connection and writes on it.

require "acemq/amqp"
require "acemq/amqp/patterns"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "accounts-events"
QUEUE = "accounts.opened"

# A database with one table of accounts and one of outbox records, and a
# transaction that either keeps both or keeps neither. A store is anything
# answering `add`, `pending` and `mark_published` — there is no base class to
# inherit and no registration to do.
class Ledger
  def initialize
    @accounts = []
    @outbox = []
    @lock = Mutex.new
  end

  def transaction
    @lock.synchronize do
      accounts = @accounts.dup
      outbox = @outbox.dup
      begin
        yield self
      rescue StandardError
        # Both tables go back. This is the whole property: the message and the
        # work it describes are one write.
        @accounts = accounts
        @outbox = outbox
        raise
      end
    end
  end

  def insert_account(account) = @accounts << account
  def accounts = @accounts.map { |a| a["account_id"] }

  # ---- the outbox store seam ----
  def add(record) = @outbox << record
  def pending(limit = 0) = limit.zero? ? @outbox.dup : @outbox.first(limit)
  def mark_published(id) = @outbox.reject! { |record| record.id == id }
  def waiting = @outbox.size
end

mq = Connection.open(URL, origin: "examples/03-outbox")

[QUEUE, Naming.dead_letter_queue(QUEUE)].each do |queue|
  mq.delete_queue(queue) if mq.queue_exists?(queue)
end

Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(QUEUE, dead_letter: true)
        .binding(QUEUE, EXCHANGE, "account.#")
        .apply(mq)

arrived = Thread::Queue.new
consumer = mq.consume(QUEUE) do |message|
  arrived << message
  Ack.accept
end

ledger = Ledger.new

# The committed case. A record holds already-encoded bytes rather than an
# object, because it outlives the process that wrote it and the class may not
# survive the deployment that happens while it waits.
ledger.transaction do |db|
  db.insert_account({ "account_id" => "A-1" })
  db.add(Patterns.record(mq, { "account_id" => "A-1" },
                         to: "account.opened", exchange: EXCHANGE,
                         type: "account.opened.v1"))
end

# The rolled-back case, which is the one that is hard to notice going wrong: no
# account, and no message about an account.
begin
  ledger.transaction do |db|
    db.insert_account({ "account_id" => "A-2" })
    db.add(Patterns.record(mq, { "account_id" => "A-2" },
                           to: "account.opened", exchange: EXCHANGE,
                           type: "account.opened.v1"))
    raise "the compliance check failed"
  end
rescue RuntimeError => e
  puts "rolled back: #{e.message}"
end

puts "accounts committed: #{ledger.accounts.inspect}"
puts "outbox holds:       #{ledger.waiting}"

# `sweep` is public so an application can flush at the end of a request rather
# than up to an interval later, and so this example does not have to sleep. In
# a service you would `relay.start` and leave it running.
relay = Patterns::OutboxRelay.new(mq, ledger, interval: 0.2)
published = relay.sweep
puts "relay published:    #{published}"

message = nil
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
until message || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
  begin
    message = arrived.pop(true)
  rescue ThreadError
    sleep 0.05
  end
end

relay.close
consumer.cancel

puts "arrived:            #{message&.payload.inspect}"
puts "type:               #{message&.envelope&.type}"

mq.close

# The relay is deliberately at-least-once: a record is removed only after the
# broker has confirmed it, so a crash in between sends the message again.
# Anything that consumes an outbox needs to be idempotent, which is why that
# pattern is in the same library. Removing first would lose messages instead,
# and an absence cannot be recognised the way a duplicate can.
abort "the outbox kept the rolled-back message" unless ledger.waiting.zero?
abort "expected the relay to publish 1, got #{published}" unless published == 1
abort "nothing arrived on #{QUEUE}" unless message
abort "the wrong account was published" unless message.payload["account_id"] == "A-1"

# A message through the outbox is indistinguishable on the wire from one that
# was published directly: its envelope is built by the same rules `publish`
# uses, down to the origin.
abort "the outbox message lost its type" unless message.envelope.type == "account.opened.v1"
abort "the outbox message lost its origin" unless message.envelope.origin == mq.origin
