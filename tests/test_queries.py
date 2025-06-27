import json
import os
import sys
import unittest

import pandas
from pandas.testing import assert_frame_equal

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts"))
)

from queries import parse_loki_results


class LokiResultsParsingTest(unittest.TestCase):

    def test_parse_empty(self):
        df = parse_loki_results("")
        self.assertTrue(df.empty)
        self.assertListEqual(list(df.columns), ["group", "detector", "exposure"])

    def test_parse_sample(self):
        data = {
            "group": ["g1", "g2"],
            "detector": [1, 2],
            "exposure": [2025001010123, 2025020200234],
        }
        results = "\r".join(
            [
                json.dumps(
                    {"line": json.dumps({"group": g, "detector": d, "exposures": [e]})}
                )
                for g, d, e in zip(data["group"], data["detector"], data["exposure"])
            ]
        )
        df = parse_loki_results(results)
        expected = pandas.DataFrame(data=data)
        assert_frame_equal(df, expected)
