# advanced/01 — encrypting payloads

TLS protects a message while it is moving. It does nothing about one sitting in
a queue that an operator, a backup, or anybody with the management interface
can read. This encrypts the body, and rotates the key without a flag day.

## What it shows

- **The codec is the seam.** `EncryptedCodec` wraps another codec and encrypts
  what it produced, so the format and the decision to encrypt stay independent:
  JSON in, AES-GCM out.
- **The key identifier travels in the clear, in front of the ciphertext.**
- **A tampered message does not open.**
- **A keyring holds the key that writes and every key that still has to read.**

## Running it

```bash
docker compose up -d --wait
bundle exec ruby advanced/01-encrypting-payloads/main.rb
```

## What to look for

```
on the broker:  ae010f7265636f7264732d323032362d3036020a8dfa504a…  (119 bytes)
content type:   application/vnd.acemq.encrypted
readable?       no
key needed:     records-2026-06

one byte flipped: this message did not decrypt with key "records-2026-06". Either that is not the key it was written with, or it was altered after it was written.

keyring holds:  ["records-2026-06", "records-2026-09"], writing with records-2026-09
read back:      P-2 — hypertension
read back:      P-3 — hypertension
```

The hex is what the broker holds. `0f7265636f7264732d...` in the middle of it
is the key identifier — fifteen bytes of ASCII, readable without any key at
all. The diagnosis is not.

```
0xAE  0x01  len  key identifier   12-byte nonce   ciphertext + 16-byte tag
```

**The last two lines are the point.** One message was written with the old key
by a producer that has not been restarted, one with the new key, and the same
consumer read both. A consumer reads which key a message needs rather than
assuming the current one, which is what makes a rotation something you can do
on a Tuesday afternoon.

## Why the identifier is in the body

An AMQP header would have been tidier and would have lost it. Headers are
dropped by shovels, rewritten by federation links, and absent from a message
recovered out of a backup — and a ciphertext whose key nobody can name is gone.

It is also the cipher's **associated data**, so an identifier altered in flight
makes the message fail to open rather than opening as something else.

## The content type is `application/vnd.acemq.encrypted`

Not `…+json`, even though the plaintext under it is JSON. A `+json` suffix is a
promise that the bytes on the wire are JSON, and every JSON-aware consumer
reads it that way — including this library's own codec. These bytes are
ciphertext, and with the wrong name a message ends up failing inside a parser
rather than being refused by a codec that knows it cannot help.

## A body that will not open is parked, not retried

`DecodeError` is a `FatalError`, so a message this codec cannot open goes to
`medical.records.parked` rather than round the retry loop — nothing about it
will be different next time. A message that failed five times and a message
nothing could read are different problems, and mixing them means somebody
sorts them by hand.

## What still needs a decision from you

- **The keys come from `Keys.generate` here.** In production a `Keyring` is a
  small class in front of a key management service.
- **Once a queue is opaque, the people who used to debug production by reading
  a message cannot.** `EncryptedCodec.key_id_of` answers the usual question —
  which key does this need — without holding any. It is not a substitute for
  deciding how support reads a message before you turn this on.
- **Encryption is not authorisation.** Every service holding the keyring reads
  every message under those keys. Separate audiences mean separate keys.

Encrypt what genuinely needs it and leave the rest readable. A system where
every queue is opaque is a system nobody can operate.

## Interoperability

The framing here is Java's, byte for byte. **Go and .NET write different bytes
under the same content type** — Go omits the magic byte and uses a two-byte
length; .NET omits it too and uses AES-CBC with an HMAC tag. This library
interoperates with Java and with nothing else yet.
