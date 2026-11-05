# Dataset names

Datasets are named `VENUE.FEED`, following Databento. Where Databento already
names a dataset, use its name unchanged (for example `OPRA.PILLAR`,
`XNAS.ITCH`). For venues Databento doesn't cover, the feed part is the
venue's own name for the feed.

Add a row here before writing data under a new name, so every deployment
uses the same one.

| Dataset | Venue | Feed |
|---|---|---|
| `COINBASE.EXCHANGE` | Coinbase | Coinbase Exchange market data (websocket) |
| `KRAKEN.SPOT` | Kraken | Kraken spot market data |
| `KRAKEN.FUTURES` | Kraken | Kraken derivatives market data |
| `ALPACA.IEX` | Alpaca | IEX market data as served by Alpaca |
| `ALPACA.SIP` | Alpaca | Consolidated (SIP) US equities data as served by Alpaca |

Schema names are lower case, using Databento's names where the meaning
matches (`trades`, `mbp-1`, `mbo`, `definition`). tinytrader's own schemas are
listed with their extensions once they are defined.

| Dataset | Schema | Records |
|---|---|---|
| `ALPACA.SIP` | `cmbp-1` | Consolidated quotes (action `M`, the quoting exchanges as each side's publisher) and trades (action `T`, the executing exchange, the book before it), merged in event order, trades first on a tie |
| `ALPACA.IEX` | `mbp-1` | IEX's own book and trades, one publisher |
| `ALPACA.SIP`, `ALPACA.IEX` | `definition` | The day's instrument definition |
| `ALPACA.SIP`, `ALPACA.IEX` | `conditions` | `tinytrader.trade-conditions/1` (rtype `0xD4`, byte-identical to Hyphae's `hyphae.dbn.equity-conditions@1`): the nth record belongs to the nth trade of the day's main file |
| `ALPACA.SIP` | `quote-conditions` | `tinytrader.quote-conditions/1` (rtype `0xD5`, 32 bytes; `libs/alpaca/src/quote_conditions.rs` documents the layout): each quote's condition codes and tape, the nth record belonging to the nth quote (action `M`) of the day's main file; flag bit 0 marks a quote whose conditions couldn't be held (no codes kept) |

These days come from Alpaca's history, filed one file per symbol, schema and
day ([0014](decisions/0014-history-feeds.md)). Publisher ids: `40001` IEX,
`40002` the SIP itself (a quote's header), and `40100 + (letter − 'A')` for
each exchange by Alpaca's one-letter code (`V`, IEX, is 40121).
