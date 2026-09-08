# basic/04 — four formats on one queue

JSON, YAML, TOML and XML published to one queue and read by one consumer. This
is what a format migration looks like from the inside: the new producer is
deployed on Tuesday, the old one is still running on Wednesday, and the
consumer has to read both without a flag day.

## What it shows

- **`CompositeCodec` writes with the first codec it was given and reads with
  whichever answers.** So a migration is one line in the consumer, deployed
  ahead of the producers that need it.
- **The content type decides, not the bytes.** Nothing is sniffed. Each codec
  reads more content types than it writes — `application/x-yaml`, `text/yaml`,
  `text/x-yaml` and `…+yaml` as well as `application/yaml` — because a producer
  in another stack uses whichever spelling its own library picked.
- **Only JSON and bytes answer for a message with no content type at all.** A
  sender that said nothing is almost always sending JSON, and a codec that
  volunteered there would be right about the value and wrong about the format.
- **`codec:` on a single publish overrides the connection's**, which is how one
  service publishes the new format while still reading the old.

## Running it

```bash
docker compose up -d --wait
bundle exec ruby basic/04-codecs/main.rb
```

## What to look for

```
application/json     {"sku" => "X-1", "name" => "Ratchet", "price" => 1299}
application/toml     {"sku" => "X-1", "name" => "Ratchet", "price" => 1299}
application/xml      {"sku" => "X-1", "name" => "Ratchet", "price" => "1299"}
application/yaml     {"sku" => "X-1", "name" => "Ratchet", "price" => 1299}

registered codecs: ["bytes", "json", "string", "toml", "xml", "yaml"]
```

**Look at `price` in the XML row.** It comes back as the string `"1299"` where
the other three give the integer. XML has only text, and nothing in the
document says which of the two it was. That is a property of the format rather
than of this library, and it is the sort of thing better discovered in an
example than in a consumer that has quietly started rounding.

## Two refusals worth knowing about

**`YAMLCodec` parses with `Psych.safe_load`, never `YAML.load`.** A message
body is untrusted input, and `YAML.load` on untrusted input is remote code
execution. `Date`, `Time` and `DateTime` are permitted by default, because Java
and Go both write timestamps; `Symbol` and YAML aliases have to be asked for.

**`XMLCodec` refuses a `<!DOCTYPE>` outright**, and that is not configurable. A
document type declaration is how an XML body reads files off the machine
handling it and expands a few bytes into a heap full of them.

## What is not in the Gemfile, and why

The gem declares **no runtime dependencies**. YAML is Psych and the TOML reader
and writer are written into the library, but XML needs REXML — which ships with
Ruby and has been a *bundled* gem since 3.4, so a Bundler process does not get
it for free. That is why `rexml` is in this repository's Gemfile and why the
error names it when it is absent.
