# frozen_string_literal: true

# Ten requests in flight at once, each getting its own answer, and a responder
# failure reaching the caller as an exception rather than as a timeout.
#
# This is a synchronous shape drawn on top of an asynchronous system, and that
# is a real cost rather than a free convenience: a caller blocked on a reply is
# holding a thread, a connection and a deadline, and a responder that backs up
# turns into a caller that stops responding. Reach for it where the caller
# genuinely cannot go on without the answer, and publish an event otherwise.

require "acemq/amqp"
require "acemq/amqp/patterns"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

REQUESTS = "pricing.requests"

CATALOGUE = { "X-1" => 1299, "X-2" => 450, "X-3" => 8999 }.freeze

mq = Connection.open(URL, origin: "examples/01-request-reply")

mq.delete_queue(REQUESTS) if mq.queue_exists?(REQUESTS)
mq.declare_queue(REQUESTS)

# The responder returns the answer, not an Ack. Raising is how it says the
# request could not be answered, and the failure travels back to whoever is
# waiting — which is the whole reason to raise rather than to return an error
# object nobody remembered to check.
responder = Patterns.serve(mq, REQUESTS, concurrency: 4) do |message|
  sku = message.payload["sku"]
  price = CATALOGUE[sku]
  raise "no such product: #{sku}" unless price

  { "sku" => sku, "price" => price }
end

# A requester is meant to be kept and reused. It holds a reply queue and a
# consumer, so one per request is a queue per request. Without `reply_to:` it
# generates an exclusive, transient, auto-deleting queue that goes away with
# this process — that one is classic, necessarily, because RabbitMQ replicates
# nothing that disappears with its connection.
prices = Patterns::Requester.new(mq, to: REQUESTS, timeout: 10)
puts "reply queue: #{prices.reply_queue}"

# Ten at once, on ten threads, to show that the correlation identifier is what
# pairs a reply with its request rather than the order they come back in.
answers = Array.new(10) do |n|
  Thread.new { prices.call({ "sku" => CATALOGUE.keys[n % 3] }) }
end.map(&:value)

answers.each_with_index do |answer, n|
  puts format("  request %2d  %s -> %d", n + 1, answer["sku"], answer["price"])
end

# And the failure. The responder raises; the caller sees it.
failure = nil
begin
  prices.call({ "sku" => "NOPE" })
rescue Patterns::ResponderFailed => e
  failure = e
end
puts "the responder's failure reached the caller: #{failure&.message}"

prices.close
responder.cancel
mq.close

abort "expected 10 answers, got #{answers.size}" unless answers.size == 10
unless answers.all? { |answer| CATALOGUE[answer["sku"]] == answer["price"] }
  abort "an answer went to the wrong caller"
end
abort "the responder's failure did not reach the caller" unless failure
