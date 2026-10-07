# frozen_string_literal: true

require "securerandom"
require "acemq/amqp"
require_relative "contracts"

module Policies
  # Documents: the claim-check pattern.
  #
  # A medical report scanned at 300 dpi is tens of megabytes. Putting it on a
  # queue is possible and is a mistake -- it fills the broker's memory, it is
  # copied to every bound queue, it makes a dead-letter queue impossible to
  # inspect, and it turns a broker into a filesystem with worse tools. What
  # travels instead is a *claim check*: the document goes to a store, and the
  # message carries the key it was stored under.
  #
  # The store here is a Hash, because the example must run without
  # infrastructure. A real one is S3, Azure Blob Storage or a filesystem -- the
  # pattern is identical and only two method bodies change.
  #
  # **Retention is the part people forget.** The store and the queue have
  # different lifetimes. A message replayed a month later carries a key, and if
  # the store expired that key at thirty days the replay produces a message
  # nobody can read -- worse than a lost message, because it looks like a
  # message. Store retention must exceed every retention that could bring a
  # message back, dead-letter queues and manual replay included.
  class DocumentModule
    def initialize(connection)
      @mq = connection
      @store = {}
      @lock = Mutex.new
    end

    # Stores a document and announces that it exists.
    #
    # @param content [String] the bytes, which go nowhere near the broker
    # @return [String] the key the event carries
    def store(policy_id, kind, content)
      key = "doc/#{policy_id}/#{kind}/#{SecureRandom.uuid[0, 8]}"
      @lock.synchronize { @store[key] = content }
      # The event is a few hundred bytes whatever the document weighs.
      @mq.publish(DocumentStored.new(policy_id: policy_id, document_key: key, kind: kind,
                                     bytes: content.bytesize).to_wire,
                  to: DOCUMENT_STORED, exchange: EXCHANGE, mandatory: true,
                  type: DocumentStored.type, correlation_id: policy_id)
      key
    end

    # Redeems a claim check.
    #
    # @return [String, nil] the document, when the store still has it
    def fetch(key) = @lock.synchronize { @store[key] }

    # How many documents are held.
    def held = @lock.synchronize { @store.size }
  end
end
