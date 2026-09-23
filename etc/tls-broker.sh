#!/usr/bin/env bash
# Writes development certificates and starts a broker that serves TLS from
# them, for advanced/02-development-certificates.
#
# The certificates cannot be committed and cannot be written by compose: they
# are private keys with a thirty-day life, and the broker's rabbitmq.conf names
# the files it was given. So this generates them with the library's own
# generator — the same one the example demonstrates, and the same one Go's
# acemq-certs and .NET's AceMq.Amqp.DevCerts write — and only then brings the
# broker up.
#
# CI runs this too, rather than a TLS setup of its own, so a broker that works
# on a laptop and one that works on a runner cannot quietly differ.
set -euo pipefail

cd "$(dirname "$0")/.."

# No lock file is committed, deliberately — .gitignore says why — and one left
# on disk from an earlier run is never something anybody chose. Bundler honours
# it, `bundle install` reports success, and everything from here on runs against
# whatever release the lock happens to name. Nothing says so: the floor check
# reads the Gemfile rather than the lock, and the examples pass against an older
# library just as green as against the current one. A contributor then reports
# having run the suite against a release they have not run it against.
#
# So take it away and resolve afresh. On a runner there is never one to remove,
# which is the point — a laptop and a runner should not be resolving different
# libraries from the same Gemfile.
if [ -f Gemfile.lock ]; then
  echo "removing a stray Gemfile.lock; this repository resolves afresh every run"
  rm -f Gemfile.lock
fi
bundle install --quiet

echo "writing certificates into certs/"
bundle exec ruby -e '
  require "acemq/amqp"
  result = AceMQ::AMQP::DevelopmentCertificates.generate(
    directory: "certs", broker_host: "localhost", days: 30
  )
  puts "  #{result.files.length} files, expiring #{result.expiry.utc}"
'

# The broker reads the certificates as root inside the container and the
# generator writes the keys 0600, owned by whoever ran this. RabbitMQ runs as
# the rabbitmq user, so the key has to be readable by it — 0644 on a key that
# lives for thirty days and signs nothing outside this machine.
chmod 0644 certs/server.key certs/client.key

echo "starting the brokers"
docker compose --profile tls up -d --wait

echo
echo "plain AMQP  amqp://guest:guest@localhost:5672"
echo "TLS         amqps://guest:guest@localhost:5671   (certs/ca.crt)"
# A third broker, for advanced/04-blocked-broker. It raises a real memory alarm,
# and an alarm stops every connection on the broker that publishes — so it gets
# one of its own instead of a turn on the one above.
echo "blockable   amqp://guest:guest@localhost:5673   (advanced/04 only)"
