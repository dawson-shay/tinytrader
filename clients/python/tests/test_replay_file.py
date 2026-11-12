"""A relay replay file read by databento's own reader.

The fixture is written by the relay's writer (services/relay/src/replay.rs,
the_python_round_trip_fixture_is_what_the_writer_writes): a symbol mapping,
five trades and the replay's end, its DBN header's schema unset (mixed).
A reader that trusted a one-schema header read the mapping and system records
as that schema's and failed; databento must read this one, given the schema.
"""

import pathlib
import unittest

try:
    import databento
except ImportError:  # CI installs it (.github/workflows/ci.yml, python-client).
    databento = None

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "replay-mixed.dbn.zst"


def store():
    # From bytes: the store then holds no open file (CI runs with
    # ResourceWarning as an error).
    return databento.DBNStore.from_bytes(FIXTURE.read_bytes())


@unittest.skipIf(databento is None, "databento isn't installed")
class ReplayFileTest(unittest.TestCase):
    def test_header_says_mixed(self):
        s = store()
        self.assertIsNone(s.metadata.schema)
        self.assertEqual(s.metadata.dataset, "OPRA.PILLAR")

    def test_trades_read_back_given_the_schema(self):
        df = store().to_df(schema="trades")
        self.assertEqual(len(df), 5)
        self.assertEqual(sorted(df["size"].tolist()), [1, 2, 3, 4, 5])

    def test_no_schema_given_is_refused_not_misread(self):
        with self.assertRaises(ValueError):
            store().to_df()


if __name__ == "__main__":
    unittest.main()
