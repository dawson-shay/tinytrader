# Recovering a deployment

A deployment holds state that can't be rebuilt from code: ledgers, order
journals, strategy state, secrets and market data. This page inventories what
the platform does and doesn't yet do to protect that state against losing a
machine's disk, and who decides each part.

The platform provides mechanisms. Each deployment chooses the schedule, the
repository, the recipients and what leaves its premises (see
[CONTRIBUTING](../CONTRIBUTING.md)).

## What a deployment holds

| Kind | Examples | Rebuildable? |
|---|---|---|
| Code and configuration | Repositories, host configuration | Yes, from git and the lock file |
| Secrets | Encrypted files, the key that opens them | The files, yes. The key, no |
| Service state | Ledgers, journals, reminders, strategy state | No |
| Market data | Captured and purchased datasets | Only if purchased again |
| Caches | Build outputs, downloaded catalogs | Yes |

## After a power cut

On macOS the Nix store is a volume of its own, mounted a little after launchd
starts the boot's daemons. A job whose program is in `/nix/store` then fails
to start. launchd marks it `EX_CONFIG` (78), stops trying, and `KeepAlive`
doesn't bring it back. So every native service the platform defines starts
through `/bin/wait4path /nix/store` first, as nix-darwin's own `command` jobs
do: it waits for the store, then becomes the service.

- A deployment's own launchd jobs that set `ProgramArguments` should do the
  same: tinytrader's flake exports `lib.launchd.waitForNix`.
- A job that launchd gave up on before this fix stays down until someone
  starts it: `launchctl print system/<label>` shows `last exit code = 78`, and
  `sudo launchctl kickstart -k system/<label>` (or `gui/<uid>/<label>` for an
  agent) starts it.

## Gaps

Each gap names what's missing and where the fix belongs.

**Code**

- *Unpushed or uncommitted work is invisible.* Nothing reports commits that
  exist only on one machine. **Platform:** a check that lists them across a
  deployment's repositories.
- *Pinned revisions can be unpublished.* A deployment's lock file may pin a
  revision that is never pushed, so a fresh clone can't build. **Platform:**
  the deployment check should fail when a locked revision isn't on the remote.

**Secrets**

- *One key opens everything.* The default setup encrypts every secret to a
  single key. Losing that key loses all secrets. **Platform:** documentation
  and a check for multiple recipients. **Deployment:** which recipients, and
  where a recovery key is kept.
- *Secrets are not mirrored.* Nothing outside the encrypted files holds a copy
  of the key. **Deployment:** an offline copy.

**Service state**

- *No service declares its state as durable.* A backup can't tell ledgers from
  caches. **Platform:** a service option naming its durable paths, which
  backup tooling can read.
- *No platform backup.* Nothing in the platform backs up service state.
  **Platform:** a backup module built on an encrypted, deduplicating tool, with
  retention and verification. **Deployment:** repository location, schedule
  and retention.
- *A deployment path is hard-coded.* The order gateway refuses state roots
  under a particular volume path, and its tests use that path. **Platform:**
  the refused paths should come from configuration, not from the source.

**Market data**

- *No copy policy.* The store records where data is, not how many copies exist
  or on which kinds of medium. **Platform:** copy counts per dataset in the
  catalog. **Deployment:** which data earns which copies.

**Scheduling and alerts**

- *Nothing runs backups.* No timer or launcher is part of the platform.
  **Platform:** a job template for backups (the queue's templates can carry
  it). **Deployment:** the schedule.
- *No freshness signal.* A stale backup looks the same as a current one.
  **Platform:** a backup-age metric in the observe contract, and a notice when
  it exceeds a threshold.

**Multiple machines**

- *The single-writer rule is only a convention.* Two machines can run the same
  stateful service and write the same journal. **Platform:** a lease or a
  refusal to start a service whose state is held elsewhere. **Deployment:**
  which machine owns each service.

**Restore**

- *No restore drill.* Backups haven't been tested by restoring them.
  **Platform:** verification steps, such as a gateway journal's chain check,
  that a drill can run on a restored copy. **Deployment:** the drill's schedule.

## Who decides what

| Decision | Owner |
|---|---|
| Backup tool and format | Platform |
| Which state is durable | Platform (per service), confirmed by the deployment |
| Repository location, schedule, retention | Deployment |
| Recipients, recovery key, where it's kept | Deployment |
| Whether any copy leaves the premises | Deployment |
| Restore drill schedule | Deployment |
