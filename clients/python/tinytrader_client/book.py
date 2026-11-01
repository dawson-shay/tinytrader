"""The top of book of every instrument the data stream has quoted: a view over
delivered records, kept for convenience. A strategy that wants something else
keeps its own from ``on_record``."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from functools import lru_cache
from zoneinfo import ZoneInfo

NEW_YORK = ZoneInfo("America/New_York")

UNDEF_PRICE = 2**63 - 1
# DBN rtypes (dbn's RType).
MBP_1 = 0x01
SYMBOL_MAPPING = 0x16
CMBP_1 = 0xB1
CBBO_1S = 0xC0
CBBO_1M = 0xC1
TCBBO = 0xC2
BBO_1S = 0xC3
BBO_1M = 0xC4
QUOTES = {MBP_1, CMBP_1, CBBO_1S, CBBO_1M, BBO_1S, BBO_1M}


@dataclass(frozen=True, slots=True)
class Quote:
    """A top of book: prices in dollars, ``None`` for a side that isn't there;
    ``ts`` when it was received (ns), for its age."""

    bid: float | None
    ask: float | None
    bid_size: int
    ask_size: int
    ts: int

    @property
    def mid(self) -> float | None:
        if self.bid is None or self.ask is None:
            return None
        return (self.bid + self.ask) / 2

    @property
    def usable(self) -> bool:
        """Both sides there and not crossed."""
        return self.bid is not None and self.ask is not None and self.bid <= self.ask


def compact(symbol: str) -> str:
    """An OPRA raw symbol without its padding: ``SPY   261027C00580000`` →
    ``SPY261027C00580000``."""
    return symbol.replace(" ", "")


# Futures roots the gateway knows (order-gateway.md, section 6), dollars per point.
FUTURES = {"ES": 50, "MES": 5, "NQ": 20, "MNQ": 2, "RTY": 50, "M2K": 5, "YM": 5}


def future(symbol: str) -> tuple[str, int, int] | None:
    """Root, month and year digits of a futures outright of a known root
    (``ESZ6``, ``MESH27``), else None."""
    if not 3 <= len(symbol) <= 8 or not all(c.isascii() and (c.isupper() or c.isdigit()) for c in symbol):
        return None
    digits = len(symbol) - len(symbol.rstrip("0123456789"))
    if digits not in (1, 2) or len(symbol) < digits + 2:
        return None
    head, year = symbol[:-digits], symbol[-digits:]
    root, code = head[:-1], head[-1]
    month = {"H": 3, "M": 6, "U": 9, "Z": 12}.get(code)
    if month is None or root not in FUTURES:
        return None
    return root, month, int(year)


@lru_cache(maxsize=1 << 16)
def occ(symbol: str) -> tuple[str, dt.date, str, float] | None:
    """Root, expiry, ``C``/``P`` and strike of a compact OCC symbol, else None."""
    s = compact(symbol)
    if len(s) < 16 or s[-9] not in "CP" or not s[-8:].isdigit() or not s[-15:-9].isdigit():
        return None
    ymd = s[-15:-9]
    try:
        expiry = dt.date(2000 + int(ymd[:2]), int(ymd[2:4]), int(ymd[4:]))
    except ValueError:
        return None
    return s[:-15], expiry, s[-9], int(s[-8:]) / 1000


def _px(raw: int) -> float | None:
    return None if raw == UNDEF_PRICE else raw / 1e9


class Book:
    """Instruments by id (from symbol mappings and definitions) and their last
    quote. A ``tcbbo`` record's quote (the one before its trade) counts only for an
    instrument no quote record has quoted, as in the simulator.

    OPRA assigns instrument ids per day, so an id can name another contract
    tomorrow: a mapping that gives an id to a new symbol forgets the symbol it
    named, quote included. An option whose expiry is before today (New York) is
    never served. Each symbol is parsed once, and options are indexed by root and
    expiry, so ``chain`` touches only its expiry."""

    def __init__(self) -> None:
        self.symbols: dict[int, str] = {}
        self._ids: dict[str, int] = {}
        self._quotes: dict[int, Quote] = {}
        self._quoted: set[int] = set()
        self._occ: dict[int, tuple[str, dt.date, str, float]] = {}
        self._index: dict[tuple[str, dt.date], set[int]] = {}
        self.today: dt.date | None = None

    def set_now(self, ns: int) -> None:
        """The session's time. At a new New York day every quote is cleared, as in the
        simulator, so nothing is served from yesterday's book; and expired contracts
        drop out. Futures trade through midnight: their quotes stay."""
        today = dt.datetime.fromtimestamp(ns / 1e9, NEW_YORK).date()
        if self.today is not None and today > self.today:
            keep = {i for i, s in self.symbols.items() if future(s) is not None}
            self._quotes = {i: q for i, q in self._quotes.items() if i in keep}
            self._quoted &= keep
        self.today = today

    def apply(self, rec) -> None:
        """Takes one delivered record (a ``databento_dbn`` record)."""
        rtype = int(getattr(rec, "rtype", -1))
        iid = rec.instrument_id
        if rtype == SYMBOL_MAPPING:
            self._map(iid, rec.stype_out_symbol)
        elif hasattr(rec, "raw_symbol") and hasattr(rec, "strike_price"):
            self._map(iid, rec.raw_symbol)
        elif rtype in QUOTES or rtype == TCBBO:
            if rtype == TCBBO and iid in self._quoted:
                return
            if rtype != TCBBO:
                self._quoted.add(iid)
            lv = rec.levels[0]
            ts = rec.ts_recv if rtype != MBP_1 else rec.ts_event
            self._quotes[iid] = Quote(_px(lv.bid_px), _px(lv.ask_px), lv.bid_sz, lv.ask_sz, ts)

    def _forget(self, iid: int) -> None:
        sym = self.symbols.pop(iid, None)
        if sym is not None and self._ids.get(compact(sym)) == iid:
            del self._ids[compact(sym)]
        self._quotes.pop(iid, None)
        self._quoted.discard(iid)
        o = self._occ.pop(iid, None)
        if o is not None:
            self._index.get((o[0], o[1]), set()).discard(iid)

    def _map(self, iid: int, symbol: str) -> None:
        key = compact(symbol)
        was = self.symbols.get(iid)
        if was is not None and compact(was) != key:
            self._forget(iid)  # the id named another contract
        prev = self._ids.get(key)
        if prev is not None and prev != iid:
            self._forget(prev)  # the contract had another id
        self.symbols[iid] = symbol
        self._ids[key] = iid
        o = occ(key)
        if o is not None:
            self._occ[iid] = o
            self._index.setdefault((o[0], o[1]), set()).add(iid)

    def _live(self, iid: int) -> bool:
        o = self._occ.get(iid)
        return o is None or self.today is None or o[1] >= self.today

    def quote(self, symbol: str) -> Quote | None:
        """The last quote of a symbol (raw or compact OCC, or a stock); ``None`` for
        an option past its expiry."""
        iid = self._ids.get(compact(symbol))
        if iid is None or not self._live(iid):
            return None
        return self._quotes.get(iid)

    def expiries(self, root: str) -> list[dt.date]:
        """The expiries of a root's quoted options, today's and later, in order."""
        return sorted(
            e for (r, e), ids in self._index.items()
            if r == root and (self.today is None or e >= self.today)
            and any(i in self._quotes for i in ids)
        )

    def chain(self, root: str, expiry: dt.date) -> dict[tuple[float, str], Quote]:
        """One expiry's quoted options: ``(strike, "C"|"P") → Quote``; empty once
        the expiry has passed."""
        if self.today is not None and expiry < self.today:
            return {}
        out = {}
        for iid in self._index.get((root, expiry), ()):
            q = self._quotes.get(iid)
            if q is not None:
                o = self._occ[iid]
                out[(o[3], o[2])] = q
        return out
