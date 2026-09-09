# frozen_string_literal: true

# A payload too large for a broker, put aside, and the reference that travels
# instead.
#
# A forty-megabyte message is possible and is a mistake: it sits in the broker's
# memory, it is copied to every queue bound to the exchange, and it turns a
# dead-letter queue into something nobody can open. What travels instead is a
# claim check — the payload goes to a store, and the message carries the key.
#
# The threshold is the part worth watching rather than the offloading. Below it
# a payload travels inline, exactly as it would without this codec, because
# turning a two-hundred-byte event into a store round trip *and* a broker round
# trip makes the common case slower in order to fix the rare one. Three bytes at
# the front of the body say which of the two a message is, so a consumer reads
# both without being told which to expect — which is what lets the threshold be
# changed, or this codec be introduced, on a queue that already has messages in
# it.

require "acemq/amqp"
require "acemq/amqp/patterns"
require "fileutils"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "documents-events"
QUEUE = "documents.invoices"
PARKED = Naming.parked_queue(QUEUE) # => "documents.invoices.parked"

# Where the payloads go. Beside this file so it can be looked at while the
# example runs; in a service it is the mount, the volume or the bucket that
# every publisher and every consumer can reach.
PAYLOADS = File.join(__dir__, "payloads")

# An ordinary invoice. A couple of hundred bytes, which is what nearly all of
# them are, and the case an unconditional claim check would make worse.
INVOICE = {
  "invoice" => "INV-2231",
  "supplier" => "Northwind Paper",
  "total_cents" => 8450,
  "lines" => [{ "sku" => "A4-80GSM", "quantity" => 40 }]
}.freeze

# The same invoice with the supplier's scan attached, base64 in the body the way
# an attachment usually arrives. Half a megabyte is an unremarkable scan and
# eight times the threshold.
SCANNED = INVOICE.merge(
  "invoice" => "INV-2232",
  "scan" => ["%PDF-1.4\n#{"\0" * (384 * 1024)}"].pack("m0")
).freeze

THRESHOLD = Patterns::ClaimCheckCodec::DEFAULT_THRESHOLD

# The three bytes at the front of a body, which are what a consumer reads to
# decide what it is holding.
def framing(body) = body[0, 3].unpack("C3").map { |byte| format("%02x", byte) }.join(" ")

# Answered from the body alone, holding no store at all. It is the line an
# operator wants in front of a dead-letter queue: which object does this message
# need, and is it still there?
def claim_check?(body) = !Patterns::ClaimCheckCodec.key_of(body).nil?

# Nothing here has a blocking read with a deadline that works on Ruby 3.1, so
# this polls — which is also the shape that lets the example fail with something
# readable rather than hang until CI kills it.
def pull_within(connection, queue, seconds: 15)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
  while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
    delivery = connection.pull(queue)
    return delivery if delivery

    sleep 0.05
  end
  abort "nothing arrived on #{queue} within #{seconds} seconds"
end

FileUtils.rm_rf(PAYLOADS)

# The filesystem store rather than the in-memory one, and that choice is the
# pattern rather than a detail. `InMemoryClaimCheckStore` keeps the payloads in
# the publisher's own memory — which is where they were going to be anyway — so
# a consumer in another process finds nothing at all. What makes a claim check
# work is the payload outliving the process that wrote it.
#
# A directory is the honest middle ground: right where the filesystem is shared
# and durable, and no better than the hash on a container's local disk. Object
# storage is the usual answer in a deployment, and a store in front of S3 is the
# same three methods.
store = Patterns::FilesystemClaimCheckStore.new(PAYLOADS)
codec = Patterns::ClaimCheckCodec.wrapping(JSONCodec.new, store)

mq = Connection.open(URL, origin: "examples/08-claim-check", codec: codec)

# This example counts what is in the store and what reaches the parked queue, so
# it starts from empty queues: a message left behind by an earlier run would be
# counted too, and the checks at the bottom would fail for a reason that has
# nothing to do with claim checks.
[QUEUE, Naming.dead_letter_queue(QUEUE), PARKED].each do |queue|
  mq.delete_queue(queue) if mq.queue_exists?(queue)
end

Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(QUEUE, dead_letter: true)
        .binding(QUEUE, EXCHANGE, "invoice.#")
        .apply(mq)

mq.publish(INVOICE, to: "invoice.received", exchange: EXCHANGE, type: "invoice.received.v1")
mq.publish(SCANNED, to: "invoice.received", exchange: EXCHANGE, type: "invoice.received.v1")

# Pulled rather than consumed, so the bodies can be looked at as the broker
# holds them and then decoded by hand. The broker has no idea any of this
# happened: it holds two messages, and one of them is 39 bytes.
inline = pull_within(mq, QUEUE)
checked = pull_within(mq, QUEUE)
inline_body = inline.body.to_s.b
checked_body = checked.body.to_s.b
inline.ack
checked.ack

puts "small invoice:   #{inline_body.bytesize} bytes on the wire, " \
     "framing #{framing(inline_body)}, claim check: #{claim_check?(inline_body)}"
puts "with the scan:   #{checked_body.bytesize} bytes on the wire, " \
     "framing #{framing(checked_body)}, claim check: #{claim_check?(checked_body)}"
puts "key on the wire: #{Patterns::ClaimCheckCodec.key_of(checked_body)}"

held = Dir.children(PAYLOADS).sort
puts "in the store:    #{held.inspect}"

# Both come back through the same codec, and it was not told which was which —
# the framing said.
read_back = [inline_body, checked_body].map { |body| codec.decode(body) }
read_back.each do |invoice|
  scan = invoice.fetch("scan", "")
  puts "read back:       #{invoice["invoice"]}, #{scan.bytesize} bytes of scan"
end
puts

# ---------------------------------------------------------------------------
# The boundary itself, with no broker in the way. The comparison is strictly
# less than, so a payload of exactly the threshold is the first one offloaded —
# the same number and the same comparison in all five libraries, because a
# threshold that differed by language would mean two services disagreeing about
# which messages are claim checks.
overhead = JSONCodec.new.encode({ "scan" => "" }).bytesize
below = codec.encode({ "scan" => "x" * (THRESHOLD - 1 - overhead) })
at_threshold = codec.encode({ "scan" => "x" * (THRESHOLD - overhead) })

puts "#{THRESHOLD - 1} bytes encoded: #{below.bytesize} on the wire, " \
     "claim check: #{claim_check?(below)}"
puts "#{THRESHOLD} bytes encoded: #{at_threshold.bytesize} on the wire, " \
     "claim check: #{claim_check?(at_threshold)}"
puts

# ---------------------------------------------------------------------------
# And what the pattern costs. The store and the queue have separate lifetimes
# and nothing enforces a relationship between them, so a payload can be removed
# while a message referring to it is still deliverable. Here that is a directory
# being emptied; in a deployment it is a lifecycle rule on a bucket, or a volume
# reclaimed along with a pod.
mq.publish(SCANNED, to: "invoice.received", exchange: EXCHANGE, type: "invoice.received.v1")
FileUtils.rm_f(Dir.glob(File.join(PAYLOADS, "*")))

# It is fatal rather than retryable, and the difference matters: the payload is
# not coming back, so a message redelivered for it only holds a queue open until
# it ages out. A body that will not decode never reaches the handler at all, and
# the consumer settles it itself — parked rather than dead-lettered, because a
# message nothing could read is a different problem from one that failed five
# times.
handled = Thread::Queue.new
consumer = mq.consume(QUEUE) do |message|
  handled << message
  Ack.accept
end

deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
while mq.message_count(PARKED).zero?
  break if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

  sleep 0.1
end
consumer.cancel

parked = pull_within(mq, PARKED)
reason = Envelope.from_headers(parked.headers, parked.routing_key).error
parked.ack
puts "payload gone:    #{reason}"

mq.close
FileUtils.rm_rf(PAYLOADS)

# The sizes either side of the threshold are the claim, so they are checked
# rather than printed. An example that printed whatever it got would go on
# passing after the threshold stopped being applied.
abort "a payload under the threshold was put in the store" if claim_check?(inline_body)
abort "a payload over the threshold travelled on the broker" unless claim_check?(checked_body)
unless checked_body.bytesize < inline_body.bytesize
  abort "the reference is not the smaller of the two: #{checked_body.bytesize} bytes"
end
abort "expected one payload in the store, found #{held.inspect}" unless held.size == 1
unless held.include?(Patterns::ClaimCheckCodec.key_of(checked_body))
  abort "the key on the wire is not the one the store issued"
end
unless read_back.map { |invoice| invoice["invoice"] } == %w[INV-2231 INV-2232]
  abort "an invoice did not come back: #{read_back.map { |i| i["invoice"] }.inspect}"
end
abort "the stored payload did not come back byte for byte" unless
  read_back[1]["scan"] == SCANNED["scan"]
abort "#{THRESHOLD} is no longer compared strictly less than" if
  claim_check?(below) || !claim_check?(at_threshold)
abort "a message whose payload was gone reached the handler" unless handled.empty?
abort "the parked message does not say what went wrong: #{reason.inspect}" unless
  reason.include?("is not in the store")
