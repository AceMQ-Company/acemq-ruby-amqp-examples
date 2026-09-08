# frozen_string_literal: true

# A TLS connection to a broker with its own certificate authority, the
# development certificates that make one easy to stand up, and the refusal that
# stops them reaching production.
#
# Every certificate the generator writes carries
# "ACEMQ DEVELOPMENT ONLY - DO NOT TRUST" in its subject organisation, and this
# library refuses one that does — however trust is configured, unverified mode
# included — unless it is told to allow them, in a separate and visible step. A
# self-signed authority that drifts into production is *worse* than no
# encryption, because everything looks protected and nothing is verified.
#
# Run ./etc/tls-broker.sh first: it writes the certificates and starts a broker
# that serves TLS from them.

require "acemq/amqp"
require "tmpdir"
require "time"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

TLS_URL = ENV.fetch("ACEMQ_TLS_URL", "amqps://localhost:5671")
CA = ENV.fetch("ACEMQ_TLS_CA", "certs/ca.crt")

EXCHANGE = "secure-events"
QUEUE = "secure.pings"

unless File.readable?(CA)
  abort "no certificate authority at #{CA}. Run ./etc/tls-broker.sh, or point " \
        "ACEMQ_TLS_CA at one."
end

# The password does not go in the URL. A URL is the one piece of configuration
# that gets printed — into error messages, structured logs, `ps` output,
# whatever the deployment tool echoes back — and a password that has been
# through any of those has to be rotated.
credentials = Credentials.of(username: ENV.fetch("ACEMQ_TLS_USERNAME", "guest"),
                             password: ENV.fetch("ACEMQ_TLS_PASSWORD", "guest"))
puts "credentials:  #{credentials.inspect}"
puts

# ---------------------------------------------------------------------------
# 1. Naming an authority narrows trust to that authority alone; the system
#    store is not consulted at all. That is the point — a certificate from a
#    public authority is not evidence that the thing answering is *your*
#    broker, and the hundreds of authorities a machine trusts by default are
#    hundreds of ways to be wrong.
#
#    And this is refused, because the authority carries the marker.
refused = nil
begin
  Connection.open(TLS_URL, security: Security.verified(certificate_authority: CA),
                           credentials: credentials).close
rescue ConfigurationError => e
  refused = e
end

puts "refused, before a socket was opened:"
puts "  #{refused&.message}"
puts

# ---------------------------------------------------------------------------
# 2. Said out loud, it connects. A separate, visible step rather than a
#    keyword, for the same reason `without_verifying_the_broker` has a long
#    name: it has to be legible in a diff. It weakens nothing else —
#    verification stays on, and a certificate that does not verify is still
#    refused.
security = Security.verified(certificate_authority: CA).allowing_development_certificates

mq = Connection.open(TLS_URL, security: security, credentials: credentials,
                              origin: "examples/02-development-certificates")

[QUEUE, Naming.dead_letter_queue(QUEUE)].each do |queue|
  mq.delete_queue(queue) if mq.queue_exists?(queue)
end

Topology.new
        .exchange(EXCHANGE, :topic)
        .queue(QUEUE, dead_letter: true)
        .binding(QUEUE, EXCHANGE, "ping.#")
        .apply(mq)

arrived = Thread::Queue.new
consumer = mq.consume(QUEUE) do |message|
  arrived << message
  Ack.accept
end

mq.publish({ "sent_at" => Time.now.utc.iso8601 },
           to: "ping.sent", exchange: EXCHANGE, type: "ping.sent.v1")

message = nil
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
until message || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
  begin
    message = arrived.pop(true)
  rescue ThreadError
    sleep 0.05
  end
end

consumer.cancel
puts "over TLS:     #{message&.payload.inspect}"
puts

# ---------------------------------------------------------------------------
# 3. What the generator writes. Into a temporary directory rather than over
#    `certs/`, because regenerating changes the authority and the broker
#    started from the old one is still running.
Dir.mktmpdir("acemq-certs") do |directory|
  result = DevelopmentCertificates.generate(directory: directory, broker_host: "localhost",
                                            days: 30)

  puts "the generator writes:"
  result.files.sort.each { |path| puts "  #{File.basename(path)}" }
  puts "expires:      #{result.expiry.utc}"
  puts "authority:    #{result.authority.subject}"

  # Keys are 0600 and everything is short-lived, because a development
  # certificate that never expires is one that outlives the reason it was
  # created.
  mode = File.stat(File.join(directory, "ca.key")).mode & 0o777
  puts "ca.key mode:  #{format("%04o", mode)}"

  abort "a key was written world-readable" unless mode == 0o600
  abort "the marker is not in the subject" unless
    result.authority.subject.to_s.include?("DO NOT TRUST")
end

mq.close

abort "the development authority was accepted without being allowed" unless refused
abort "nothing arrived over TLS" unless message
