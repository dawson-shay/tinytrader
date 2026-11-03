# Sessions (`tinytrader.session/1`)

| | |
|---|---|
| **Purpose** | One run of a strategy against held days: the sources merged on one clock, served over the relay protocol, simulated venues as ordinary gateways, everything advanced in lockstep ([decision 0022](../decisions/0022-sessions.md)). |
| **Transport** | Control: TCP NDJSON. Data: `tinytrader.relay/1` (a JSON join line, then DBN v3). Orders: `tinytrader.gateway/1`, one gateway per venue. All on loopback unless `--listen` says otherwise. |
| **Versioning** | Every control message is in `tinytrader.session/1`; `hello` names it. |
| **Auth** | The control and data addresses are the run's own and go unauthenticated, as the relay's do; each venue's gateway token is in `welcome`. |
| **Errors** | A run that can't go on ends with `end{status: "incomplete", reason}`, and the process exits 1. |
| **Stability** | **Experimental** until 0022's later steps (paper and live sessions) land. |

A session is a process per run, not a service. Backtest mode is built; `paper` and `live` are later steps.

## Running one

```sh
tinytrader-session run session.toml --dir runs/2026-10-27-a
# READY session=5f1c… mode=backtest control=127.0.0.1:53001 data=127.0.0.1:53002 orders=options=127.0.0.1:53003
```

The strategy reads the control address (from the `READY` line, or `TT_SESSION` set from it) and finds everything else in `welcome`. The run folder must not be under `/tmp`, as for any gateway state. It ends up holding:
- `run.json`, the run record;
- `audit.log`;
- `config.toml`;
- each venue's journal under `venues/<name>/`.

## Configuration

```toml
days = ["2026-10-26", "2026-10-27"]   # in order
strategy_latency_ms = 0               # the strategy's declared latency
ready_timeout_s = 60                  # per step, for the first connections, and for every write
seed = 0                              # each venue's seed derives from it and its name (latency jitter)
calendar = "calendar.json"            # optional: the market calendar for the venues

[[sources]]                           # in rank order: ties in time go to the earlier
name = "opra"
holdings = "store:http://127.0.0.1:9120"   # or "job:DIR" (a databento-jobs cache job), "folder:DIR" (the store's layout)
                                      # or a list, each day read from the first that holds it
provider = "databento"
dataset = "OPRA.PILLAR"
symbol = "SPY.OPT"
schema = "cbbo-1m"
latency_offset_ns = 0                 # added to ts_event where F_BAD_TS_RECV is set
expiries_within_days = 30             # optional: options expiring more than N calendar days later, or already expired, are left out

[[venues]]                            # one simulated account, one gateway
name = "options"
quotes = ["opra"]                     # the sources it fills against
underlyings = ["SPY"]
equities = []
futures = []                          # futures roots allowed: "ES", "MES", ...
max_future_qty = 1                    # contracts per opening futures order
max_notional = 2500                   # dollars per order
max_open = 4
allow_short = false
[venues.sim]                          # the simulator's profile: order-gateway.md §10's script keys
latency_ms = 5
fee_per_contract = "0.65"
```

**`calendar`** is the market calendar: `{"days": [{"date", "open", "close"}]}` with times in New York, as the deployment's market calendar serves it (`alpaca-account`'s `/calendar`, say). It's given to every venue whose profile sets none, so holidays have no session and early closes close early, for orders, day-order expiry and option settlement. Its SHA-256 is in `run.json`. Without it, venues assume every weekday from 09:30 to 16:00, holidays included.

Sources are read through the store producer (`libs/producer`). A day not held is a `store-gap` message in the stream, never a skip. Each file read is pinned by SHA-256 in the run record.

## Starting the strategy, and the sandbox

With a `[strategy]` table the session starts the strategy itself. Its output goes to `strategy.out` and `strategy.err` in the run folder. Without the table, nothing changes: start the strategy yourself from the `READY` line.

**Its environment starts empty**, then gets:
- `PATH=/usr/bin:/bin`;
- `HOME` and `TMPDIR`, both its scratch folder in the run folder;
- the configured `env`;
- `TT_SESSION`, the control address;
- `TT_SESSION_TOKEN`.

Nothing else of the session's environment reaches it: no keys, store addresses or agent sockets. The control connection must present the token in `hello`, so the record's claims are about the process the session started.

**It runs in its own process group**, which is killed after the run, so nothing it started outlives it. A grandchild that calls `setsid` leaves the group and isn't killed, but it stays sandboxed.

```toml
[strategy]
command = ["python3", "-m", "my_strategy"]
cwd = "/path/to/code"                    # optional; with a sandbox, inside allow_read
env = { PYTHONPATH = "/path/to/code" }   # optional
sandbox = "macos"                        # or "off", the default
allow_read = ["/path/to/code", "/path/to/venv", "/path/to/python"]
deny_read = ["/path/to/local/caches"]    # never readable, even inside allow_read
```

`sandbox = "macos"` runs the strategy under `sandbox-exec` with a profile the session writes (`sandbox.sb` in the run folder, and in `run.json`). It denies by default, then allows:
- reading the system's folders: `/System`, `/usr`, `/bin`, `/sbin`, `/private/etc`, dyld and timezone data, `/nix/store`, but not `/Library`, whose logs can carry market data;
- reading the `allow_read` folders;
- reading and writing its scratch folder and the harmless devices;
- connecting to the session's control and data addresses and its venues' gateways, and nowhere else;
- signalling only processes in its own sandbox.

**What it denies:**
- **Mach service lookups, all of them.** So the strategy can't ask an unsandboxed service (Launch Services' `open`, launchd's `launchctl submit`, background downloads, the DNS resolver) to act for it.
- **The `deny_read` folders,** denied last, so they win over an `allow_read` that contains them. So are the per-user folders that hold caches, keys and configuration: `~/.cache`, `~/Library`, `~/.ssh`, `~/.gnupg`, `~/.config`, `~/.aws` and `~/.netrc`. An `allow_read` inside or containing one is refused.
- **Unsafe `allow_read` folders.** Each must exist (it's made canonical), and none may be or contain `/`, `/Users`, `/Volumes`, `/Library`, `/private`, `/var`, `/var/lib/tinytrader`, the user's home, or a `deny_read` folder. A folder inside home, such as a virtualenv, is fine.

Held data, caches, home and the store's API are therefore out of reach: a sandboxed strategy sees the market only through the session, as decision 0026 §5 asks of an actor. `allow_read` must name everything the strategy loads: its code, and its interpreter and libraries when they live outside the system's folders. `/nix/store` is readable; it holds no plaintext secrets in a sops deployment.

**The run record says how the strategy ran.** `strategy_process` holds:
- the command;
- its exit status;
- what the record attests: the control connection presented the token only that process had. That assumes no other process of the same user read the token from the child's environment and connected first;
- `sandbox`: `"off"`, or its kind, the profile's text and SHA-256, `allow_read` and `deny_read`.

So a result shows whether its strategy could have read ahead.

**Limits:**
- `sandbox-exec` is deprecated by Apple but works on current macOS.
- On Linux a container with networking off can't reach the session's TCP addresses; that needs the session and its gateways on unix sockets, a later step.
- A strategy that exits before the run ends leaves the run incomplete.

## The control connection

| Direction | Message |
|---|---|
| strategy → session | `{"type":"hello","protocol":"tinytrader.session/1","strategy":NAME,"revision":REV,"token":TT_SESSION_TOKEN?}`. The token is required when the session started the strategy. |
| session → strategy | `{"type":"welcome","session","mode":"backtest","data":ADDR,"orders":{VENUE:{"addr","client","token","prefix"}},"days","strategy_latency_ms"}` |
| strategy → session | `{"type":"ready","as_of":0,"sent":{},"timers":[…]}`, once joined to the data stream and every venue, with its first timers |
| session → strategy | `{"type":"step","step":N,"as_of":TS,"records":K,"timers":[ID],"reports":{VENUE:SEQ}}` |
| strategy → session | `{"type":"ready","as_of":TS,"sent":{VENUE:COUNT},"timers":[{"at":TS,"id":ID}]}` |
| session → strategy | `{"type":"end","status","reason","audit","steps","records","reports":{VENUE:SEQ}}` |

**A step** is the next time anything happens: the records of one index time (decision 0009's order: `ts_recv`, or `ts_event` plus the source's offset where flagged), timers due then, or both. For each step the session:
1. pushes the records to the venues that quote their source;
2. advances every venue to `as_of`, once it has handled the order messages the strategy last said it sent (venue reports, class 2);
3. sends `step`, whose `timers` are the timers due at `as_of` (class 3);
4. writes the records to the data stream. They go after the step, so a step bigger than a socket's buffer can't block the session while the strategy waits for the step.

**The strategy:**
1. reads `records` records from the data stream;
2. reads each venue's gateway events up to the `reports` sequence number, so it has every fill of the step before acting on it;
3. acts;
4. answers `ready`.

**`sent`** counts the submits and cancels the strategy has sent to each venue since it first connected, valid or not, resent ones included; queries don't count ([order-gateway.md §10](order-gateway.md#10-simulated-broker)). It must be exact. Each venue reports how many it has handled when it advances, and a count higher than `sent` means an order the strategy didn't count, which could have been stamped on either side of the advance. Orders take effect at `as_of` plus the strategy's and the venue's latencies.

**Timers** are set in `ready`, each for a time after the step's and no later than the last day's end. A timer at the step's own time is refused rather than moved: decision 0009 orders timers after market data at one time, and that step has passed.

**After the last record**, the venues are advanced to the end of the last day, so day orders expire. After a short pause the venues are asked again, so an order the strategy sent but didn't count, still in flight at the last advance, fails the run too. `end` follows, with each venue's last event, so the strategy can read the last day's expiries before closing.

**The run ends `incomplete`** when the strategy:
- doesn't answer within `ready_timeout_s`;
- stops reading its control or data connection for that long (every write has that timeout);
- answers for another time;
- sends a `sent` that goes back, or that doesn't match what a venue handled;
- sets a timer outside the run.

## The data stream

Join as for a relay: `{"protocol":"tinytrader.relay/1","client":NAME}`. The session sends a DBN v3 header, then each step's records unchanged, as the store holds them:
- each day's symbol mappings at the day's start;
- the stored records;
- `store-gap` and `store-partial` notices.

With one source, the header's schema is that source's. With several, it's unset (mixed), and each record's rtype says what it is.

## A Rust strategy in process

A strategy written in Rust implements `session::actor::Actor` and can run
either way (decision 0029), with the same run to the audit hash:

- **In process:** `Session::run_inproc(Box::new(actor))`, in place of
  `Session::run`. The session calls the actor with each step on a thread of
  its own. Its orders go to each venue's gateway through an in-process client
  (`Gateway::connect_local`), which takes a TCP connection's path: the same
  authentication, welcome, parsing, inbox and admission, so nothing is `busy`
  under the session's clock. Its `ready.sent` is counted for it, from the
  submits and cancels it sends. A run config with `[strategy]` is refused: an
  in-process run starts no strategy process.
- **Over the sockets:** `session::actor::run_socket(actor, control, token)`
  speaks this page's protocols as a client: `hello`, the data stream, each
  venue's gateway over TCP, and a `ready` for every step.

An actor's step has its records as DBN record bytes (`ActorStep::records`
decodes them in order), the timers due, and each venue's last report
sequence number. It reads its venue's reports up to that number before it
acts, as a socket strategy does. `session::example::Toy` is the fixture's toy
written this way. Run in process and over the sockets, it reproduces the
golden audit hash and `run.json` recorded from the socket toy.

## The run record and the audit hash

`run.json` (`tinytrader.session-run/1`) holds:
- the status and reason;
- the audit hash;
- the steps and records;
- the strategy's name and revision;
- the days and the seed;
- the lockstep batch and strategy latency;
- the merge's rank order;
- the configuration's SHA-256;
- per source: its holdings, records, clamps, each day's files with their SHA-256s, and the files being read when a run stopped part way;
- per venue: the simulator model, its profile, the sources it quotes, and the order messages sent.

The **audit hash** is a SHA-256 over `audit.log`, which records:
- the strategy's `hello`;
- every step (its time, each source's span of positions, the timers fired, each venue's count of journaled order events). The count is used rather than the last event number, which also counts connection events that land by wall time;
- every `ready`;
- the final advance;
- each venue's journaled order events (submits, cancels, broker order events), without their sequence numbers and hash-chain links, which also count connection events.

The log is written as the run goes, never held whole. The same configuration, the same held days and the same strategy give the same hash. A strategy that reconnects to a gateway mid-run still gets the same hash, but its connection events aren't checked.
