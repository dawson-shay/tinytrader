"""Order intents in the gateway's form (order-gateway.md, section 6). Prices are
decimal strings; the gateway refuses a market order that opens a position."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation


def _price(p: float | str | Decimal) -> str:
    """A price in cents, exactly: one that isn't a whole number of cents, or is
    zero, is refused rather than rounded."""
    try:
        d = Decimal(str(p))
    except InvalidOperation as e:
        raise ValueError(f"not a price: {p!r}") from e
    cents = d.quantize(Decimal("0.01"))
    if cents != d:
        raise ValueError(f"price {p!r} isn't a whole number of cents")
    if cents == 0:
        raise ValueError("a limit of 0 isn't a price")
    return str(cents)


def _instrument(symbol: str) -> dict:
    from .book import future

    s = symbol.replace(" ", "")
    if future(s) is not None:
        return {"class": "future", "symbol": s}
    is_option = len(s) > 15 and s[-9] in "CP" and s[-8:].isdigit()
    return {"class": "option" if is_option else "equity", "symbol": s}


def single(symbol: str, side: str, effect: str, qty: int, limit: float | str, tif: str = "day") -> dict:
    """One leg: ``side`` ``buy``/``sell``, ``effect`` ``open``/``close``."""
    return {
        "shape": "single",
        "instrument": _instrument(symbol),
        "side": side,
        "effect": effect,
        "qty": qty,
        "type": "limit",
        "limit_price": _price(limit),
        "time_in_force": tif,
    }


def combo(legs: list[tuple[str, int, str, str]], qty: int, limit: float | str) -> dict:
    """Several option legs, each ``(symbol, ratio, side, effect)``, at a net
    ``limit`` per unit: positive a debit, negative a credit."""
    return {
        "shape": "combo",
        "legs": [
            {"instrument": _instrument(s), "ratio": r, "side": side, "effect": eff}
            for s, r, side, eff in legs
        ],
        "qty": qty,
        "type": "limit",
        "limit_price": _price(limit),
        "time_in_force": "day",
    }
