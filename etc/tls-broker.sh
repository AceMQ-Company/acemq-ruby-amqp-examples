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
