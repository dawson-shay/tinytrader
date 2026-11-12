# 0040: Live capture recorders, raw first

- **Status:** Proposed
- **Date:** 2026-11-11
- **Concretises:** [0013](0013-lossless-capture-and-hot-swap.md) (lossless
  capture)
- **Builds on:** [0035](0035-content-addressed-store.md) (objects),
  [0039](0039-storage-contract-and-fsck.md) (the contract),
  [0037](0037-time-conventions.md) (clock), [0018](0018-partial-coverage.md)
  (holes in coverage)
- **Built after** the NAS (2026-11-15) and 0035's core. First venues:
  Coinbase and Kraken.

## Context

A CEX's live order-book history can't be bought afterwards. Whatever we don't
record is gone. The pilots on `newdrive` from early October recorded Coinbase
and Kraken, and they're prior art, not the design. The user's priority, after
the NAS, is live collection of CEX and other data.

## Decision

### 1. Raw first: every message as received

- **One recorder per venue connection** writes every message exactly as
  received, with:
  - `ts_recv`: our clock, UTC nanoseconds, with its offset recorded;
  - the venue's own timestamp;
  - the venue's sequence number;
  - the connection id.
- **Append-only chunk files:** each record is length-prefixed, and the file
  is flushed often (every second, at most). A chunk is **sealed** every few
  minutes (or at a size): fsynced, hashed, and written as an object with its
  provenance (venue, endpoint, subscription, recorder version, host, clock
  offset). The sealed chunk is then bound in the volume's log (0039 S1–S3).
- **Never normalised in the recorder.** The recorder parses only what it must
  to track sequence numbers and gaps. Everything else is bytes.

### 2. Normalisation is a separate, replayable job

- **A normaliser reads raw chunks and writes DBN** (trades, book deltas,
  snapshots) as objects, with provenance naming its input chunks and its own
  version.
- **Replayable:** a normaliser bug costs a rerun over the raw chunks, never
  data. Raw chunks are `irreplaceable` (0034). Normalised days are
  rebuildable from them.
- **Books** rebuild from a snapshot plus deltas. The normaliser checks each
  venue's checksum where there is one (Kraken's book checksum, for example),
  and a mismatch is a hole, not a guess.

### 3. Gaps are recorded, never smoothed

- **Detected:** a sequence jump, a reconnect, a heartbeat missed past its
  interval, or a venue's own gap notice.
- **Recorded as holes** in coverage (0018), with their start, end and cause,
  so `/coverage` says exactly what's missing.
- **After a reconnect** the recorder resubscribes and takes a fresh snapshot.
  The normaliser starts a new book from it.

### 4. Dual recorders, optional

- **Two recorders on different hosts or networks** (the NAS and the M4, or a
  second uplink) record the same subscription independently.
- **Merged by sequence number:** the normaliser takes each sequence once.
  Where one recorder has a gap the other doesn't, the hole closes. Where both
  disagree on a message's bytes, it's an event, and both are kept.
- **Not required to start.** One recorder per venue, on the NAS, comes first.

### 5. Clock discipline

- **chrony on every recording host,** against more than one source.
- **The offset is recorded** in each chunk's provenance, and as a time series
  of its own.
- **A host whose offset exceeds a bound** (say 5 ms) marks its chunks
  `clock_suspect`; it doesn't stop recording.

### 6. Where it runs

- **On the NAS, under systemd:** one unit per venue recorder, each a small
  Rust binary declared in Nix (not Nomad-supervised).
- **Chunks are written to a local volume first,** then sealed into the CAS,
  so a store restart never stops recording.
- **A recorder restart** loses only the seconds the restart takes. That
  window is recorded as a hole. A dual recorder covers it, if one runs.
- **Hot swap (0013):** a new recorder version starts beside the old one,
  overlaps by a sealed chunk, and only then is the old one stopped. The
  overlap is merged by sequence.

## Steps

1. **The chunk format and recorder** for Coinbase: raw records, sealing,
   objects and provenance, gap detection and holes.
2. **Kraken,** with its book checksum.
3. **The normaliser** to DBN, replayable, with book rebuilds.
4. **Clock offsets** recorded, and `clock_suspect`.
5. **A dual recorder,** and the merge by sequence.
6. **Hot swap** by overlap.

## Open questions

- **Which channels per venue** (full L2 or L3 where offered, trades, ticker)
  and which symbols: the user's call. The recorder records whatever it's
  subscribed to.
- **The pilots' data on `newdrive`:** whether to import it as raw chunks
  (its format permitting) or keep it as is. Either way, nothing reads the
  drive directly: it comes through the store.
