# The storage contract: `tinytrader.storage/1`

What a volume in the content-addressed format must satisfy, whatever program
wrote it (decisions [0035](decisions/0035-content-addressed-store.md) and
[0039](decisions/0039-storage-contract-and-fsck.md)). Each rule has an id that
checkers report against. Changing a rule changes the version. The reference
implementation is `libs/cas` (`tinytrader-cas`), and it depends on nothing
else in tinytrader.

## Layout

Under the volume's root:

| Path | Is |
|---|---|
| `cas/FORMAT` | `{"format": "tinytrader.storage/1", "volume": "<id>"}`: the format, and the volume's id, unique among the deployment's volumes |
| `cas/objects/<aa>/<bb>/<sha256>.<ext>` | one object; `aa` and `bb` are its hash's first two bytes, in hex |
| `cas/index/bindings.jsonl` | the bindings log |
| `cas/index/snapshot.json` | the index as of a length of the log: a cache |
| `cas/tmp/` | objects being written; anything here after a crash is garbage |

## Rules

**S1, objects.**
- An object's name is the lowercase hex SHA-256 of its bytes, plus an
  extension that says its format (`dbn.zst`, `json`, `parquet`, …).
- It's written under `tmp/`, flushed (`fsync`), renamed into place, its
  folder flushed, and made read-only.
- An object is never modified or replaced. One content is one object.
- Nothing under `objects/` is a link.

**S2, the bindings log.**
- It's append-only, with one JSON event a line, each line flushed before the
  write it records is reported done.
- Events:
  - `bind`: `name`, `version`, `sha256`, `size`, `ext`, `kind`, `source`,
    `previous`;
  - `pending`: `sha256`, `ext`, `for`;
  - `retire`: `name`, `sha256`, `why`;
  - `verified`: `sha256`.
  - Each event also carries `writer` (program and version) and `at` (Unix
    seconds).
- A name's versions are bound in order: the first is 1, and each later one
  is the last plus one, with `previous` set to the last one's hash. An event
  that breaks this is a conflict: it's reported, never applied. The same
  binding written twice is not a conflict.
- Damage: a torn last line, from a crash mid-append, is the only damage
  tolerated. It's ignored and reported. The next append ends it with a
  newline first. Any other line that isn't an event is an error.
- **The index is a cache.** It's what the log says when replayed in order,
  and it's never the only record of anything. A snapshot that can't be read
  is ignored, and the log replayed whole.

**S3, provenance.** Every object bound under a name has a provenance record
(its source, its request, when it was fetched or received, the upstream
manifest and hashes, and the writer and version), itself an object. *Bound
beside its object when filing goes through the index (0035 step 3).*

**S4, formats.**
- Market data is DBN, with its version in its header.
- Reference tables are Parquet.
- Raw source bytes are kept exactly as received.
- An unknown format is held and reported, never rewritten.

**S5, identity.** Names and symbols resolve through the security master
(0038).

**S6, never silent.** Every failure to verify is an event and a mark on the
copy. Nothing counts as held without at least one verified copy.

**S7, protection.**
- At least 2 verified copies on independent disks.
- At least 3 for `irreplaceable` data (0034).
- A copy on a cold drive counts, marked offline.

## Checking

A volume is sound when:
- every name's object is there, at its size and hash;
- every object is named, pending, or named by an earlier version
  (unreferenced, kept until an operator's garbage collection);
- no object is misnamed;
- the log has no conflicts.

An object that no binding ever named is an **orphan**. It's kept, and
reported. `tinytrader-cas`'s `Volume::verify` checks S1 and S2. `tt-fsck`
(0039) runs it against every volume, with copy counts across them.

Volumes merge by name: the highest version wins, and two different hashes at
one version are a conflict, reported, with the name left out until an
operator resolves it.
