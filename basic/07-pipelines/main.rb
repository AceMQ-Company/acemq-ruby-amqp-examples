# frozen_string_literal: true

# An order carried through three services by an itinerary it brings with it —
# and a failed run put back where it stopped rather than at the beginning.
#
# No step knows what comes next. Each does its one job and hands the message
# back, and the slip on the message says where it goes, which makes the order of
# the steps a property of the message rather than something compiled into three
# services that then have to be redeployed together.
#
# The second half is the part that pays for the pattern. A run that fails at the
# second step does not start again at the first: the message records how far it
# got, so putting it back means resuming, and the step that already charged
# somebody does not charge them twice.

require "acemq/amqp"
require "acemq/amqp/patterns"
require "json"

include AceMQ::AMQP # rubocop:disable Style/MixinUsage

URL = ENV.fetch("ACEMQ_URL", "amqp://guest:guest@localhost:5672")

# The itinerary form: every step names its own destination, so nothing but the
# message is needed to follow it. These sit on the default exchange, which is
# why each step's exchange is empty and its routing key is a queue name.
SLIP_QUEUES = %w[rb-slip-validate rb-slip-charge rb-slip-ship].freeze

# The declared form. `Pipeline` names it all: the exchange is the pipeline, the
# routing key is the step, and the queue behind a step is `pipeline.step`. That
# naming is Java's exactly, and not arranged for Ruby's convenience — it is what
# lets a Ruby step stand in a pipeline a Java service declared.
PIPELINE = Patterns::Pipeline.new("rb-fulfilment", %w[validate charge ship])

def await(seen, within: 15)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + within
  while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
    begin
      return seen.pop(true)
    rescue ThreadError
      sleep 0.05
    end
  end
  nil
end

mq = Connection.open(URL, origin: "examples/07-pipelines")

# ---------------------------------------------------------------------------
# One: an itinerary assembled for this message and carried by it.
Topology.new
        .queue(SLIP_QUEUES[0], dead_letter: true)
        .queue(SLIP_QUEUES[1], dead_letter: true)
        .queue(SLIP_QUEUES[2], dead_letter: true)
        .apply(mq)

visited = []
shipped = Thread::Queue.new

carried = SLIP_QUEUES.zip(%w[validate charge ship]).map do |queue, step|
  mq.consume(queue, &Patterns.follow_slip(mq) do |message|
    visited << step
    # What a step returns is what the next one receives, which is the
    # difference between a pipeline and three consumers that happen to publish
    # to each other.
    order = message.payload.merge("stamps" => (message.payload["stamps"] || []) + [step])
    shipped << order if step == "ship"
    order
  end)
end

itinerary = Patterns::RoutingSlip.new
                                 .step("", SLIP_QUEUES[0], name: "validate")
                                 .step("", SLIP_QUEUES[1], name: "charge")
                                 .step("", SLIP_QUEUES[2], name: "ship")
puts "itinerary: #{itinerary}"
itinerary.start(mq, { "order" => "A-1" })

delivered = await(shipped)
carried.each(&:cancel)
puts "visited:   #{visited.inspect}"
puts "shipped:   #{delivered.inspect}"

# ---------------------------------------------------------------------------
# Two: a route declared in advance, and a run that resumes half way along it.
mq.apply(PIPELINE.topology)

ran = []
arrived = Thread::Queue.new
card_works = false

following = PIPELINE.steps.map do |step|
  mq.consume(PIPELINE.queue_for(step), &PIPELINE.follow(mq) do |message|
    ran << step
    # Fatal, so it is dead-lettered rather than retried. The point here is a run
    # that stops half way, not one that waits: a card issuer refusing everything
    # will refuse again in four seconds.
    if step == "charge" && !card_works
      raise FatalError, "the card issuer is refusing everything"
    end

    arrived << message.payload if step == "ship"
    message.payload
  end)
end

puts
puts "route:     #{PIPELINE}"
PIPELINE.start(mq, { "order" => "B-2" })

# It gets as far as charge and stops there.
parked = Naming.dead_letter_queue(PIPELINE.queue_for("charge"))
deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 15
sleep 0.1 while mq.message_count(parked).zero? &&
                Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
puts "ran:       #{ran.inspect}"

# ---------------------------------------------------------------------------
# The operator's half. The message that stopped carries the route and the
# position it reached, which is the whole of what is needed to put it back where
# it was rather than at the beginning.
stopped = mq.pull(parked)
abort "nothing was dead-lettered, so there is nothing to resume" if stopped.nil?
stopped.ack

envelope = Envelope.from_headers(stopped.headers, stopped.routing_key)
resume = Patterns::RoutingSlip.from(envelope, pipeline: PIPELINE)
abort "the dead-lettered message carries no route" if resume.nil?
puts "resuming:  #{resume}"
puts "           at position #{resume.position} of #{envelope.route[Headers::ROUTE]}"

card_works = true
resume.start(mq, JSON.parse(stopped.body))

finished = await(arrived)
following.each(&:cancel)
puts "ran:       #{ran.inspect}"

(SLIP_QUEUES + PIPELINE.steps.map { |step| PIPELINE.queue_for(step) }).each do |queue|
  [queue, Naming.dead_letter_queue(queue), Naming.parked_queue(queue)].each do |name|
    mq.delete_queue(name) if mq.queue_exists?(name)
  end
end
mq.close

abort "the itinerary was not followed in order: #{visited.inspect}" unless
  visited == %w[validate charge ship]
abort "the payload did not accumulate every step: #{delivered.inspect}" unless
  delivered && delivered["stamps"] == %w[validate charge ship]

# What the resume bought. `validate` charged nothing, but `charge` did, and a
# restart would have run both again.
abort "the slip does not say validate was done: #{resume.done.inspect}" unless
  resume.done.map(&:name) == %w[validate]
abort "the resumed run does not start at charge: #{resume.next_step}" unless
  resume.next_step.name == "charge"
abort "the route position is #{resume.position}, not 1" unless resume.position == 1
abort "the run did not resume where it stopped: #{ran.inspect}" unless
  ran == %w[validate charge charge ship]
abort "the resumed run never reached the end: #{finished.inspect}" unless finished
