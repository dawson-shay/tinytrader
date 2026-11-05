# tinytrader's interfaces

tinytrader is the boundary layer between markets and your code: relays stream live market data as DBN, the store serves historic DBN, and order gateways connect to brokers (with a simulated broker that replays DBN). **Strategies are clients of these servers**; they live in your own repository, not here. This directory is the reference for every interface a client, a front end or another deployment can rely on. To start writing a client, read the [strategy client tutorial](../tutorials/strategy-client.md).

## Inventory

| Interface | Schema name | Transport | Auth | Stability |
|---|---|---|---|---|
| [Market data relay](relay.md): the stream | `tinytrader.relay/1` | TCP: a JSON join line, then DBN v3 | network | **stable** |
| [Market data relay](relay.md): HTTP | (unversioned) | HTTP JSON | network | stable; `/recent`, `/tape`, `/session` experimental |
| [Order gateway](order-gateway.md) | `tinytrader.gateway/1` | TCP NDJSON, loopback | per-client token, role and prefix | **stable** |
| [Sessions](session.md) | `tinytrader.session/1` | TCP NDJSON control, the relay stream, gateways | gateway tokens in `welcome` | experimental |
| [Order gateway journal](order-gateway.md#11-order-journal) | `tinytrader.gateway-journal/1` | files | filesystem | stable |
| [Observe endpoints](observe.md) (every service) | `tinytrader.observe/1` | HTTP JSON, Prometheus text | none (reads) | **stable** |
| [Store](store.md) | (unversioned) | HTTP JSON, DBN files | bearer token on writes | reads stable; writes experimental |
| [Fleet: Docker labels, topology file, console documents](fleet.md) | `tinytrader.fleet/1` | labels, JSON file, HTTP JSON | none | **stable** |
| [Console API](console.md) | `tinytrader.fleet/1` (topology, state) | HTTP JSON | none of its own; same-origin JSON writes | fleet routes stable; actions and optional modules experimental |
| [alpaca-account](alpaca-account.md) | (unversioned) | HTTP JSON | none (reads) | `/accounts`, `/calendar` stable; others experimental |
| [alpaca-data](alpaca-data.md) | (unversioned) | HTTP JSON | none (reads) | core stable; others experimental |
| [alpaca-news](alpaca-news.md) | (unversioned) | HTTP JSON | none (reads) | experimental |
| [alpaca-history](alpaca-history.md) | receipts `tinytrader.alpaca-history.receipts/1` | HTTP JSON | same-origin writes | experimental |
| [databento-jobs](databento-jobs.md) | (unversioned) | HTTP JSON | same-origin writes, price confirmation | experimental |
| [Job queue](queue.md) | templates `tinytrader/v1` `JobTemplate` | HTTP JSON, YAML | same-origin writes | experimental |
| [econ-calendar](econ-calendar.md) | (unversioned) | HTTP JSON | none (reads) | `/events`, `/earnings` stable |
| Trade conditions records ([datasets](../datasets.md)) | `tinytrader.trade-conditions/1` | DBN records | — | stable |
| Quote conditions records ([datasets](../datasets.md)) | `tinytrader.quote-conditions/1` | DBN records | — | experimental |
| [Predictive publications](predictive.md) | `tinytrader.predictive/1` | store records, written through the trading runtime's inbound port | as the store | experimental |

**Stable** means clients may rely on it: changes are additive (new optional fields, new endpoints, new enum values clients must tolerate), and anything breaking ships under a new schema name served alongside the old one for an announced overlap. **Experimental** means it may change with its one in-tree client (usually the console); pin a commit if you depend on it.

Each reference follows one template: purpose, transport, versioning, auth, errors, health and stability up front, then configuration, shapes with examples, and limits.

### OpenAPI documents

The HTTP endpoints `tt` uses (decision 0021) are also described as OpenAPI 3.1, in [openapi/](openapi/): `databento-jobs.json` (`/price`, `/quotes`, `/submit`), `store.json` (`/catalog`, `/coverage`, `/files`) and `console.json` (`/api/job-quotes/{id}/approve` and `/decline`). They are generated from Rust types by each service's `openapi` test, never edited by hand: requests from the handlers' own types, answers from types describing them. `cargo test` fails when a document isn't what the code generates, and when it breaks the copy on `main` (a path, method, response field or enum value removed, or a request field or parameter made required). They change additively. After changing an endpoint, regenerate and review the diff:

```sh
OPENAPI_UPDATE=1 cargo test -p databento-jobs -p store -p console openapi
```

The breaking-change comparison reads `main`'s copy with git (`OPENAPI_BASE` names another base); where there's no git history (as in `nix flake check`, or a shallow checkout) it's skipped with a notice, unless `OPENAPI_REQUIRE_BASE` is set.

## Conventions

### Schema names

Versioned formats are named `tinytrader.<name>/<major>`, and the name travels with the data: a `protocol` field in a join request, a `schema` field on every message or document, a `format` field in a file. A peer refuses a major version it doesn't serve, and says which it does. A new major version is served beside the old one for an overlap period.

### Errors

- **HTTP:** a 4xx or 5xx status and a JSON body `{"error": "<one human-readable sentence>"}`. `400` a bad request, `401` a missing or wrong token, `403` refused (another site's page, a role), `404` not there or not configured, `409` a conflict with current state, `422` well-formed but invalid, `428` a confirmation is needed, `429` a rate limit, `502` an upstream failed, `503` not ready. Some services still answer a *missing or unparsable* query parameter with the framework's plain-text `400`; treat any non-JSON error body as its status alone ([decision 0017](../decisions/0017-client-contracts.md)).
- **Line protocols:** a fatal error is one last message before the connection closes (the gateway's `{"type": "error", "code", "message"}`, the relay's DBN `ErrorMsg`). Request-level errors keep the connection open and carry a machine-readable `code` or `reason`.

### Authentication

- **Network first.** Services bind to loopback by default. Reach them over a private network or tailnet policy; never expose them publicly.
- **Bearer tokens for writes.** A service that accepts writes from other machines takes `Authorization: Bearer <token>` on writes when it's given a token (the store's `STORE_WRITE_TOKEN`); reads stay open. Clients name their own token (`JOBS_STORE_TOKEN`, `HISTORY_STORE_TOKEN`, `CONSOLE_STORE_TOKEN`).
- **Order gateway clients** authenticate in `hello` with a per-client token bound to one account, a role (`trade`, `observe`, `admin`) and an order-id prefix.
- **Secrets from files.** Every secret `NAME` can instead be given as `NAME_FILE`, a file holding it (a Docker secret, a systemd credential, a decrypted secrets file); the file wins. Gateway tokens are a mode-600 file or `env:VAR`, which reads `VAR` or `VAR_FILE`.
- **Same-origin writes.** HTTP services refuse writes from a web page of another origin (`403`), and the console also requires them to be JSON. Requests without an `Origin` (programs, curl) pass.

### Times and units

| What | Unit |
|---|---|
| Wire timestamps in DBN and the order gateway | integer **nanoseconds** since the Unix epoch, UTC (above 2^53: parse as 64-bit or BigInt) |
| Times in HTTP JSON (`updated_at`, `generated_at`, `started_at`, bar `t`) | **seconds** since the Unix epoch (float where sub-second matters) |
| Live series and event logs (`/recent`, `received_ms`) | **milliseconds** since the Unix epoch, and named so |
| Instants given by a client (`start` in a relay join) | RFC 3339 |
| Trading dates | `YYYY-MM-DD`, New York for US markets |
| Times of day | `HH:MM`, New York |
| Prices | dollars: JSON numbers in HTTP views, decimal strings in the order gateway (exact) |
| Fractions (`change_pct`, `iv`) | fractions, not percent |

A field in another unit says so in its name (`_ms`, `_ns`, `_seconds`, `_bytes`).

### Naming

- Fields are `snake_case` (the queue's Kubernetes-style job documents are the exception, in `camelCase`).
- Option contracts are compact OCC symbols (`SPY261016C00775000`); stocks are their tickers.
- Account, client, service and relay names are the deployment's own. Nothing in tinytrader expects particular names: front ends find services by the capabilities they declare.

### Every service has

The [observe endpoints](observe.md): `GET /health` (`200` when ready, `503` otherwise, with a `tinytrader.observe/1` document), `GET /metrics` (Prometheus), `GET /describe` (what it is and which capabilities it serves). Most also serve `/events` (a bounded event log). Each listens on its own `*_LISTEN` variable, else `TINYTRADER_LISTEN`, else its default port:

| Default port | Service |
|---|---|
| 7600 | a relay's stream (`RELAY_LISTEN`) |
| 7601 | an order gateway (`--listen`, required) |
| 8080 | console |
| 9105 | ticker (the quickstart's toy collector) |
| 9106 | alpaca-news |
| 9107 | alpaca-account |
| 9110 | a relay's HTTP (`RELAY_OBSERVE_LISTEN`) |
| 9120 | store |
| 9121 | databento-jobs |
| 9122 | econ-calendar |
| 9123 | queue |
| 9124 | alpaca-history |
| 9125 | alpaca-data |

## Extension points

Where a deployment customises tinytrader without changing it:

| To | Use |
|---|---|
| Add a service front ends see | A `tinytrader.observe` label, or an `observe` URL in the [topology file](fleet.md#the-topology-file) |
| Offer a capability (option chains, bars, accounts) | Declare it in `/describe`'s `streams`; front ends find it by name ([observe](observe.md#capability-names-in-streams)) |
| Rename, relabel, hide or regroup services | The topology file (`services`, `hide`, `bundles`, `panels`, `links`) |
| Choose what a gateway may trade | `--underlyings`, `--equities`, caps ([gateway](order-gateway.md#8-rate-budget-and-caps)) |
| Name the console's gateway clients | `CONSOLE_GATEWAY_CLIENT`, `CONSOLE_TRADE_CLIENT`, `CONSOLE_TRADE_PREFIX` ([console](console.md#orders-and-accounts-experimental)) |
| Curate the console's views and watchlists | `CONSOLE_PREFS_DEFAULTS` (optional module) |
| Run research and batch work | Queue templates ([queue](queue.md)) |
