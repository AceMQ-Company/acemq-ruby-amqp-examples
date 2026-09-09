# intermediate/08 — the claim check

Two invoices published on one queue. One travels whole; the other's half a
megabyte of scan goes to a store, and 39 bytes reach the broker.

## What it shows

- **Both sides of the threshold in one run.** The small invoice is inline and
  nothing is stored; the large one is a reference.
- **Three bytes at the front of the body** say which of the two a message is, so
  a consumer handles both without being told which to expect.
- **The threshold is compared strictly less than**, and the example builds a
  payload either side of it rather than asserting the number in a comment.
- **What happens when the stored payload is gone**, which is the failure this
  pattern introduces and nothing else does.

## Running it

```bash
docker compose up -d --wait
bundle exec ruby intermediate/08-claim-check/main.rb
```

## What to look for

```
small invoice:   116 bytes on the wire, framing ac 01 00, claim check: false
with the scan:   39 bytes on the wire, framing ac 01 01, claim check: true
key on the wire: e52b1c1c-5a6c-490b-af5e-ea762ec5a361
in the store:    ["e52b1c1c-5a6c-490b-af5e-ea762ec5a361"]
read back:       INV-2231, 0 bytes of scan
read back:       INV-2232, 524300 bytes of scan

65535 bytes encoded: 65538 on the wire, claim check: false
65536 bytes encoded: 39 on the wire, claim check: true

payload gone:    could not be decoded: the claim check "…" is not in the store
```

**`116` against `39`.** The two lines are printed next to each other because the
difference between them is the entire pattern, and being told about it is not
the same as seeing 39. A forty-megabyte message is possible and is a mistake: it
sits in the broker's memory, it is copied to every bound queue, and it turns a
dead-letter queue into something nobody can open.

**`ac 01 00` against `ac 01 01`.** The framing is what a consumer reads, and it
is why this codec can be introduced on a queue that already has messages in it —
a body it did not write goes to the delegate untouched. The same three bytes in
all five libraries, so a document a Java service put aside is readable here.

**`65535 bytes encoded` is inline and `65536` is not.** `DEFAULT_THRESHOLD` is
64 KiB and the comparison is `<`, so a payload of exactly 65536 is the first one
offloaded. That comparison is the one thing here that cannot be changed in one
library alone: two services that disagreed about it would disagree about which
messages are claim checks.

The inline body is 65538 — three bytes of framing on top of the payload. That
overhead is why the threshold is not zero. Offloading a two-hundred-byte event
turns one broker round trip into a store round trip *and* a broker round trip,
which makes the common case slower in order to fix the rare one.

**`payload gone`.** The store and the queue have separate lifetimes and nothing
enforces a relationship between them, so a payload can be removed while a
message referring to it is still deliverable. Here the directory is emptied; in
a deployment it is a lifecycle rule on a bucket, or a volume reclaimed along
with a pod.

It is **fatal, not retryable**. The payload is not coming back, so a message
redelivered for it only holds a queue open until it ages out. The codec raises
`DecodeError`, which is a `FatalError`, so the body never reaches the handler —
the example checks that its handler never ran — and the consumer settles the
message itself onto `documents.invoices.parked` with the reason attached. Parked
rather than dead-lettered: a message nothing could read is a different problem
from one that failed five times.

## Which store

`Patterns::FilesystemClaimCheckStore`, writing into a `payloads/` directory
beside the example, which the run deletes on its way out.

The alternative in the library is `Patterns::InMemoryClaimCheckStore`, and it is
the wrong one here for the reason that is the point of the pattern: it holds the
payloads in the publisher's own memory, which is where they were going to be
anyway, so a consumer in another process gets "the claim check is not in the
store" for every message. It is genuinely useful in a test, where the publisher
and the consumer are the same process and the thing being proved is the framing.

A directory is the honest middle ground. It is right where the filesystem is
shared and durable — an NFS mount, a persistent volume — and it is the in-memory
store with extra steps on a container's local disk, where the consumer is on
another host and finds nothing. Object storage is the usual answer in a
deployment, and a store in front of S3 is the same three methods: `put`, `get`,
`delete`.

Writes are atomic — the payload goes to a `.partial` file and is renamed into
place. Messaging is exactly the arrangement that makes a consumer fast enough to
read the key before the writer finished normal rather than unlikely, and without
that it would get a truncated payload and a parse error somewhere unhelpful.

The key is checked before it becomes a path, because a key arriving from a
message is whatever a publisher put there and `../../etc/passwd` is a key too.
The expression is the same one Java's store uses, so the two accept and refuse
the same keys over one shared mount.

## Retention is the part that goes wrong

Nothing deletes a stored payload for you, and that is deliberate. Deleting on
read breaks the second consumer of the same message; deleting on acknowledgement
breaks a replay. So when a payload may be removed is a retention decision, and
retention decisions belong to whoever owns the data — `delete` is on the store
for them to call.

What that decision has to clear is every retention that could bring a message
back: queue TTLs, dead-letter queues, and however long somebody might sit on a
message before replaying it by hand. When in doubt, longer. The `payload gone`
line is what the alternative looks like.

## The key is in the body, not a header

`ClaimCheckCodec.key_of(body)` answers "which object does this message need"
from the bytes alone, holding no store at all. It is the line worth having in
front of a dead-letter queue, where the question is whether the payload is still
there.

A consumer decides what a message is from those three bytes and never from a
header, because a header can be dropped by a shovel or a plugin and the body
cannot. `x-acemq-claim` is reserved for an application that wants to say where a
payload went in a form an operator can read; nothing here depends on it.

The content type stays the delegate's — `application/json`. A claim-checked
message is still a document; it is a document that is somewhere else.
