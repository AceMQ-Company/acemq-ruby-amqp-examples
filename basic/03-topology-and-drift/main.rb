# frozen_string_literal: true

# A topology described once, printed so it can be read before it is applied,
# then applied — and a second service that disagrees about what a queue is
# being told so rather than quietly consuming nothing.
#
# The disagreement is the point. `x-queue-type` is part of a queue's identity
# to the broker, so a Java service declaring `shipping.labels` as quorum and a
# Ruby service declaring it as classic do not negotiate: whichever starts
# second is answered PRECONDITION_FAILED and consumes nothing at all.

require "acemq/amqp"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

EXCHANGE = "shipping-events"
QUEUE = "shipping.labels"
SCRATCH = "shipping.scratch"

mq = Connection.open(URL, origin: "examples/03-topology")

# Start from nothing, because the whole example is about what a declaration
# does to a broker that already has an opinion.
[QUEUE, Naming.dead_letter_queue(QUEUE), SCRATCH].each do |queue|
  mq.delete_queue(queue) if mq.queue_exists?(queue)
end

wanted = Topology.new
                 .exchange(EXCHANGE, :topic)
                 # No kind given, so this is a durable quorum queue — the same
                 # default as `declareQueue` in Java, and identical for the same
                 # reason the rung table is.
                 .queue(QUEUE, dead_letter: true)
                 # Classic, because it was asked for. A queue that is scratch
                 # space does not need replicating, and saying so in the
                 # topology is how the next person knows it was a decision.
                 .queue(SCRATCH, queue_type: :classic)
                 .binding(QUEUE, EXCHANGE, "label.#")

# A topology is data before it is an effect. This is worth printing into a
# deployment log: it is a statement of intent, in the order a broker needs it,
# and reading it is cheaper than working out afterwards what a service did to a
# shared vhost.
#
# It is deliberately not a diff against the live broker. AMQP offers no way to
# enumerate what is there without the management API, and a plan that quietly
# guessed would be worse than one honest about being intent.
puts wanted
puts

wanted.apply(mq)
puts "applied."
puts

# Now the second service. Everything about this declaration is the same except
# the kind, which is the one thing that cannot differ.
drifted = Topology.new
                  .exchange(EXCHANGE, :topic)
                  .queue(QUEUE, queue_type: :classic, dead_letter: true)
                  .binding(QUEUE, EXCHANGE, "label.#")

refused = nil
begin
  drifted.apply(mq)
rescue TransportError => e
  refused = e
end

if refused
  puts "the broker refused the second declaration:"
  puts "  #{refused.message}"
else
  puts "the broker accepted a classic redeclaration of a quorum queue, which it should not"
end

puts
# The invalid case is caught before anything reaches the broker at all: a
# binding to a queue this topology never declares is a mistake with a name, and
# saying so here beats a message that routes nowhere in production.
incomplete = Topology.new
                     .exchange(EXCHANGE, :topic)
                     .binding("shipping.nowhere", EXCHANGE, "label.#")
puts "problems before anything is declared: #{incomplete.problems.inspect}"

mq.close

abort "the broker did not refuse the drifted declaration" unless refused
abort "an unresolved binding was not reported" if incomplete.problems.empty?
