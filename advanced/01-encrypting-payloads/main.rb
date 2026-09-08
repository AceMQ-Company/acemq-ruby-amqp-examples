# frozen_string_literal: true

# Message bodies the broker cannot read, and a keyring that can be rotated
# without a flag day.
#
# `EncryptedCodec` wraps any other codec and encrypts what it produced, so the
# broker, its disk, its backups and its management interface hold ciphertext.
# The key identifier travels in the clear in front of it — that is what makes
# rotation possible, because a consumer reads which key a message needs rather
# than assuming the current one — and the header is the cipher's associated
# data, so an identifier altered in flight makes the message fail to open
# rather than opening as something else.

require "acemq/amqp"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "medical-events"
QUEUE = "medical.records"

# In a service these come from a secret manager, not from `generate`. What is
# worth copying is the shape: one key writes, and every key that still has to
# read stays in the ring.
last_quarter = EncryptionKey.new("records-2026-06", Keys.generate(bits: 256))
this_quarter = EncryptionKey.new("records-2026-09", Keys.generate(bits: 256))

keys = Keyring.of(last_quarter.id, last_quarter.secret)

plain = JSONCodec.new
sealed = EncryptedCodec.wrapping(plain, keys)

mq = Connection.open(URL, origin: "examples/01-encrypting-payloads", codec: sealed)

# A second connection that reads the bytes as they lie on the queue, which is
# how this example shows what the broker actually holds. `BytesCodec` answers
# for anything and decodes to the body unchanged.
raw = Connection.open(URL, origin: "examples/01-encrypting-payloads", codec: BytesCodec.new)

[QUEUE, Naming.dead_letter_queue(QUEUE), Naming.parked_queue(QUEUE)].each do |queue|
  mq.delete_queue(queue) if mq.queue_exists?(queue)
end

Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(QUEUE, dead_letter: true)
        .binding(QUEUE, EXCHANGE, "record.#")
        .apply(mq)

record = { "patient" => "P-1", "diagnosis" => "hypertension", "notes" => "review in 3 months" }

mq.publish(record, to: "record.filed", exchange: EXCHANGE, type: "record.filed.v1")

# What is on the queue. Pulled rather than consumed so the bytes can be looked
# at and then put back.
delivery = raw.pull(QUEUE)
body = delivery.body.to_s.b
puts "on the broker:  #{body[0, 24].unpack1("H*")}…  (#{body.bytesize} bytes)"
puts "content type:   #{delivery.content_type}"
puts "readable?       #{body.include?("hypertension") ? "yes" : "no"}"

# The key identifier is legible without the key, which is what makes an
# operator's question — "which key does this message need?" — answerable from a
# dead-letter queue rather than from a backup.
puts "key needed:     #{EncryptedCodec.key_id_of(body)}"
puts

# Tampering. The header is the cipher's associated data and the tag covers the
# ciphertext, so a single altered byte anywhere makes the message fail to open.
# A DecodeError is a FatalError, so a message like this goes to
# `#{QUEUE}.parked` rather than round the retry loop: nothing about it will be
# different next time.
altered = body.dup
altered.setbyte(altered.bytesize - 1, altered.getbyte(altered.bytesize - 1) ^ 0x01)
tampered = nil
begin
  sealed.decode(altered)
rescue DecodeError => e
  tampered = e
end
puts "one byte flipped: #{tampered&.message}"
puts

delivery.ack

# ---------------------------------------------------------------------------
# Rotation. The new key is added and made current; the old one stays in the
# ring because messages written with it are still in flight, still on
# dead-letter queues, and still in whatever was backed up this morning.
keys.add(this_quarter)
keys.use(this_quarter.id)
puts "keyring holds:  #{keys.ids.inspect}, writing with #{keys.current.id}"

read = Thread::Queue.new
consumer = mq.consume(QUEUE) do |message|
  read << message
  Ack.accept
end

# Written with the old key, by a producer that has not been restarted.
old_writer = EncryptedCodec.wrapping(plain, Keyring.of(last_quarter.id, last_quarter.secret))
mq.publish(record.merge("patient" => "P-2"),
           to: "record.filed", exchange: EXCHANGE, type: "record.filed.v1", codec: old_writer)

# And with the new one.
mq.publish(record.merge("patient" => "P-3"),
           to: "record.filed", exchange: EXCHANGE, type: "record.filed.v1")

seen = []
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
while seen.size < 2 && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
  begin
    seen << read.pop(true)
  rescue ThreadError
    sleep 0.05
  end
end

consumer.cancel
raw.close
mq.close

seen.sort_by { |m| m.payload["patient"] }.each do |message|
  puts "read back:      #{message.payload["patient"]} — #{message.payload["diagnosis"]}"
end

abort "the body was readable on the broker" if body.include?("hypertension")
abort "the key identifier was not in the clear" unless
  EncryptedCodec.key_id_of(body) == last_quarter.id
abort "an altered body decoded anyway" unless tampered
abort "expected 2 messages back, got #{seen.size}" unless seen.size == 2

# Both keys read, which is the entire claim: a consumer holding the ring can
# read what was written before the rotation and what was written after it,
# without anybody having had to arrange for the two to happen at once.
patients = seen.map { |m| m.payload["patient"] }.sort
abort "rotation lost a message: #{patients.inspect}" unless patients == %w[P-2 P-3]
