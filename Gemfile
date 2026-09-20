# frozen_string_literal: true

source "https://rubygems.org"

# ---------------------------------------------------------------------------
# The library
#
# `acemq-amqp` is not on rubygems.org. It is published to AceMQ's own static
# feed at https://acemq.org/gems, which is a directory tree over HTTPS and
# needs no account and no credential — `source` below is the whole of what a
# user does, and it is what the library's README tells them to do.
#
# For a while these examples did not resolve it from there. The published
# 0.3.0 predated the sagas, the scheduler, the YAML, TOML, XML, Protobuf and
# Avro codecs, the encrypted bodies, the development certificates and the
# OpenTelemetry adapter — more than half of what is demonstrated here — so the
# default source was the library's `main` branch, because an example that
# cannot be run is worth very little. 0.5.0 carries every one of them, and the
# examples resolve exactly what the documentation tells you to depend on.
#
# ACEMQ_RUBY_AMQP points at a local checkout, which is how the library's own
# changes are tried against these examples before they are pushed anywhere. It
# stays: the swap above closed the gap between the release and `main`, and this
# is what keeps a future one from opening unnoticed.
#
# The floor moves with each release even though it does not have to. `~>` is
# pessimistic on the major, so `~> 0.5` and `~> 0.7` have the same ceiling and
# both resolve the newest 0.x the feed carries — this repository commits no
# lock file, so that is what every run gets either way. What the floor does is
# record which release the examples were actually run against, and with no
# lock file it is the only place that record lives. Left at 0.5 it would claim
# these examples work against a release nothing has run them against since;
# 0.7.0 changed what `close` does with its time and what a blocked connection
# reports, so that claim is not free. It also decides the failure a reader
# gets when something else in their bundle holds the version down: a
# resolution error naming the version, rather than an older library behaving
# quietly differently.
if (checkout = ENV.fetch("ACEMQ_RUBY_AMQP", nil))
  gem "acemq-amqp", path: checkout
else
  source "https://acemq.org/gems" do
    gem "acemq-amqp", "~> 0.7"
  end
end

# ---------------------------------------------------------------------------
# What the library asks for by name
#
# The gem declares no runtime dependencies at all: reading an AceMQ envelope
# should not drag a broker client into a process that will never open a socket.
# Everything below is something the library requires lazily and names in the
# error when it is absent, so an examples repository — which does open sockets,
# and does reach for the codecs — has to say so here.

# The transport. Nothing connects to a broker without it.
gem "bunny", "~> 2.23"

# Ruby 4.0 dropped `logger` from the default gems and bunny 2.24 still requires
# it without declaring it, so bunny will not load on a modern Ruby without this
# line. It is bunny's omission rather than ours, and it belongs next to bunny
# for whoever wonders why a standard-library name is in a Gemfile.
gem "logger", "~> 1.6"

# REXML ships with Ruby but has been a *bundled* gem since 3.4, which means a
# Bundler process does not get it for free. XMLCodec needs it.
gem "rexml", "~> 3.3"

# AvroCodec, and avro's own dependency. multi_json 1.20 raised its floor to
# Ruby 3.2 and 1.17 does not work with the json gem Ruby 4 ships, so it is
# pinned only where it has to be rather than everywhere.
gem "avro", "~> 1.12"
gem "multi_json", "~> 1.17.0" if RUBY_VERSION < "3.2"
# And json with it. multi_json 1.17 calls `JSON.parse` with two arguments,
# which the json gem stopped accepting at 3.0 — so on Ruby 3.1, where 1.17 is
# the newest multi_json that will install, requiring avro raises
# "wrong number of arguments" before anything of ours runs. Held at the 2.x
# line Ruby 3.1 ships with. This repository commits no lock file, so a pin that
# a lock would have supplied has to be written down here instead. It goes when
# 3.1 does, with the two lines above.
gem "json", "~> 2.6" if RUBY_VERSION < "3.2"

# ProtobufCodec. Held back on Ruby 3.1 because google-protobuf raised its floor
# to 3.2 at 4.36 — the same conditional, and the same reason, as the two
# OpenTelemetry lines below.
gem "google-protobuf", RUBY_VERSION < "3.2" ? "~> 4.29.0" : "~> 4.29"

# The tracing adapter. Held below 1.9 on Ruby 3.1 and 3.2 because
# opentelemetry-api raised its floor to 3.3 at 1.9 and the SDK did at 1.11,
# while the library still promises 3.1 — the same conditional the library's own
# Gemfile carries, for the same reason, and it goes when 3.1 does. The adapter
# itself is pure Ruby and runs on 3.1 perfectly well.
gem "opentelemetry-api", RUBY_VERSION < "3.3" ? "~> 1.8.0" : "~> 1.8"
# The SDK is not needed to emit spans — the adapter wants the API and nothing
# else. It is here because the observability example prints the spans that were
# actually recorded, and an in-memory exporter is the only way to show them
# without standing up a collector.
gem "opentelemetry-sdk", RUBY_VERSION < "3.3" ? "~> 1.10.0" : "~> 1.10"

group :development do
  # CI enforces zero offences. The examples are what people copy, so the style
  # they are written in is the style they will spread.
  gem "rubocop", "~> 1.90"
end
