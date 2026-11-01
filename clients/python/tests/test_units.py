import datetime as dt
import unittest
from types import SimpleNamespace as NS

from tinytrader_client import combo, single
from tinytrader_client.book import CBBO_1M, SYMBOL_MAPPING, Book, Quote, compact, future, occ


class Units(unittest.TestCase):
    def test_occ_symbols(self):
        self.assertEqual(compact("SPY   261027C00580000"), "SPY261027C00580000")
        self.assertEqual(occ("SPY   261027C00580000"), ("SPY", dt.date(2026, 10, 27), "C", 580.0))
        self.assertEqual(occ("SPXW  261027P05812500"), ("SPXW", dt.date(2026, 10, 27), "P", 5812.5))
        self.assertIsNone(occ("SPY"))

    def test_quotes(self):
        q = Quote(1.20, 1.24, 10, 10, 0)
        self.assertAlmostEqual(q.mid, 1.22)
        self.assertTrue(q.usable)
        self.assertFalse(Quote(1.30, 1.24, 1, 1, 0).usable)
        self.assertIsNone(Quote(None, 1.24, 0, 1, 0).mid)

    def test_intents(self):
        s = single("SPY", "buy", "open", 10, 580.1)
        self.assertEqual(s["instrument"], {"class": "equity", "symbol": "SPY"})
        self.assertEqual(s["limit_price"], "580.10")
        c = combo([("SPY261027P00570000", 1, "sell", "open"), ("SPY261027P00565000", 1, "buy", "open")], 2, -0.4)
        self.assertEqual(c["limit_price"], "-0.40")
        self.assertEqual(c["legs"][0]["instrument"]["class"], "option")
        self.assertEqual(single("SPY", "buy", "open", 1, "1.25")["limit_price"], "1.25")
        with self.assertRaises(ValueError):
            single("SPY", "buy", "open", 1, 1.234)
        with self.assertRaises(ValueError):
            combo([("SPY261027P00570000", 1, "sell", "open")], 1, -0.004)
        f = single("ESZ6", "sell", "open", 1, "7743.25")
        self.assertEqual(f["instrument"], {"class": "future", "symbol": "ESZ6"})
        self.assertEqual(future("MESH27"), ("MES", 3, 27))
        for bad in ["ESZ6-ESH7", "ESF6", "XXZ6", "Eé6", "ESZ", "SPY"]:
            self.assertIsNone(future(bad), bad)


class BookTests(unittest.TestCase):
    D27 = 1_793_059_200_000_000_000  # 2026-10-27T00:00Z
    DAY = 86_400_000_000_000

    def mapping(self, iid, raw):
        return NS(rtype=SYMBOL_MAPPING, instrument_id=iid, stype_out_symbol=raw)

    def quote(self, iid, ts, ask):
        lv = NS(bid_px=int((ask - 0.02) * 1e9), ask_px=int(ask * 1e9), bid_sz=1, ask_sz=1)
        return NS(rtype=CBBO_1M, instrument_id=iid, levels=[lv], ts_recv=ts, ts_event=ts)

    def test_a_reused_id_and_an_expired_contract_are_forgotten(self):
        b = Book()
        b.set_now(self.D27 + 15 * 3_600_000_000_000)
        b.apply(self.mapping(1, "SPY   261027C00749000"))
        b.apply(self.mapping(2, "SPY   261030C00754000"))
        b.apply(self.quote(1, self.D27, 1.25))
        b.apply(self.quote(2, self.D27, 0.50))
        self.assertAlmostEqual(b.quote("SPY261027C00749000").ask, 1.25)
        self.assertEqual(b.expiries("SPY"), [dt.date(2026, 10, 27), dt.date(2026, 10, 30)])
        self.assertEqual(set(b.chain("SPY", dt.date(2026, 10, 27))), {(749.0, "C")})
        # The next day: id 1 is a LEAP, quoted at 707.31.
        t1 = self.D27 + self.DAY + 15 * 3_600_000_000_000
        b.set_now(t1)
        b.apply(self.mapping(1, "SPY   270115C00060000"))
        b.apply(self.quote(1, t1, 707.31))
        self.assertIsNone(b.quote("SPY261027C00749000"))
        self.assertEqual(b.chain("SPY", dt.date(2026, 10, 27)), {})
        self.assertAlmostEqual(b.quote("SPY270115C00060000").ask, 707.31)
        # Yesterday's quotes are gone with the new day: only today's quoted expiry.
        self.assertEqual(b.expiries("SPY"), [dt.date(2027, 1, 15)])
        # A new day clears every quote: nothing is served from yesterday's book.
        b.set_now(t1 + self.DAY)
        self.assertIsNone(b.quote("SPY270115C00060000"))
        # A contract given a new id loses the old id's quote.
        b.apply(self.mapping(3, "SPY   261030C00754000"))
        self.assertIsNone(b.quote("SPY261030C00754000"))

    def test_a_futures_quote_stays_across_midnight(self):
        b = Book()
        b.set_now(self.D27 + 3 * 3_600_000_000_000)  # 23:00 New York, the 26th
        b.apply(self.mapping(20, "ESZ6"))
        b.apply(self.mapping(1, "SPY   261027C00749000"))
        b.apply(self.quote(20, self.D27, 7743.50))
        b.apply(self.quote(1, self.D27, 1.25))
        b.set_now(self.D27 + 5 * 3_600_000_000_000)  # 01:00, the 27th
        self.assertAlmostEqual(b.quote("ESZ6").ask, 7743.50)
        self.assertIsNone(b.quote("SPY261027C00749000"))


if __name__ == "__main__":
    unittest.main()
