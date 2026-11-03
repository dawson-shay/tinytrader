# Contributing to tinytrader

tinytrader is a **platform**. Anyone should be able to clone it and run their
own deployment without first removing someone else's choices. Please keep it
that way.

## Where your work belongs

| Belongs in tinytrader | Belongs in your own deployment repository |
|---|---|
| Libraries, services, the console | Your inventory: hosts, roles, data roots |
| NixOS and nix-darwin modules | Your topology, panels and display choices |
| Formats (topology, `tinytrader.observe/1`) and their documentation | Secrets, credentials, account identifiers |
| Generic display capabilities and formats | Strategies, research, models |
| Examples that run on their own | Decisions about your operation |

Your deployment repository takes tinytrader as a flake input and imports its
modules and packages. tinytrader never references a deployment. See
[decision 0001](docs/decisions/0001-platform-and-deployment.md).

If you're unsure, ask: *would this make sense in someone else's
deployment?* If not, it goes in yours.

## Never commit to tinytrader

- Real hostnames, tailnet names, IP addresses, or filesystem paths from a
  particular machine.
- Usernames, email addresses, account or API identifiers, secrets.
- Venues, products, strategies or schedules chosen by an operator, outside
  `examples/`.
- Links to anyone's private repositories or infrastructure.

Example values are fine inside `examples/`, because they are clearly samples
(public Coinbase products, for instance).

## Mechanisms here, choices there

tinytrader provides capabilities; deployments supply data. If you're about to
write code that checks for a particular service, metric or product name,
especially in the console, add an optional, documented field to the topology
format or a module option instead.

## Contracts

Every client-facing interface is documented in [docs/api/](docs/api/README.md), with its stability. Keep that reference in step with the code.

- **`tinytrader.observe/1`** (`libs/observe`): the `/health` document and metric
  names. Change additively; never rename or remove. Labels stay low
  cardinality (service, venue, product), never per-order ids.
- **Topology format** (read by the console): every deployment depends on it.
  Add optional fields; don't break existing files.

## Portability

Services build and run on Linux and macOS by default. Platform-specific
capability is the exception ([decision 0004](docs/decisions/0004-nix-not-nixos.md)):

- Put it behind a small interface in a library, under
  `#[cfg(target_os = "linux")]`.
- Provide an honest fallback where one exists, and report which
  implementation is in use in `/describe` and metrics.
- If there is no honest fallback, make the whole service Linux-only: target
  -specific dependencies, a clear compile error elsewhere, and
  `meta.platforms = lib.platforms.linux` on its Nix package.

## Guardrails

- `nix flake check` runs `checks.<system>.generic`, which fails on the most
  common leaks: absolute home paths, tailnet domains and private addresses.
  It is a backstop, not a substitute for the rules above.
- It also runs the Rust tests and clippy, and evaluates the example NixOS host.

## Resource bounds

- No unbounded queue, buffer, cache or log inside a service: every one has a
  fixed capacity (as `Events` and `Series` do), and says what happens when it's
  full: the sender waits (backpressure, where waiting can't deadlock), the item
  is refused with an answer (`busy`), the consumer is cut off and resumes
  (`slow_consumer`), or the run stops naming the limit. A channel is never
  `channel()` or `unbounded_channel()`.

## Shared state

- State that changes belongs to one owner: a thread or task that holds it and is
  reached by messages, as the order gateway's account session is. Others ask it;
  they don't share it.
- No shared mutable state behind a lock that more than one thread contends for.
  A lock is acceptable only where it guards a small structure with one writer and
  one reader that never wait on each other for long (a connection's outbound
  queue); say so where it's declared. A global lock over shared work (one for a
  whole store's filing, say) is a defect to shard or replace with an owner.
- Read-mostly counters and flags are atomics; values set once are `OnceLock`s.
- Services get supervisor limits by default: `resources` in the
  `tinytrader.services` module (memory, CPU, tasks, open files, restart loops,
  a capped journal) and memory caps plus log rotation in Compose files.
- Disk is bounded by the store's thresholds, never by deleting data.

## Toolchain

`rust-toolchain.toml` names the one Rust version for the repository. rustup
reads it, and Nix builds (including the container images) and dev shells
use it.

## Before you send a change

```sh
cargo fmt --all
cargo clippy --all-targets -- --deny warnings
nix flake check
```

Services must meet the service contract: configuration from environment or
flags, logs to stdout/stderr, `/health` and `/metrics` via `libs/observe`, and
clean shutdown on SIGTERM. Record decisions that shape the platform in
`docs/decisions/`.
