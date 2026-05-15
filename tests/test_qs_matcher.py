import unittest

from pipeline.qs_matcher import norm_country, parse_rank


class QSMatcherTests(unittest.TestCase):
    def test_parse_rank_uses_lower_bound(self):
        self.assertEqual(parse_rank("=15"), 15)
        self.assertEqual(parse_rank("51-100"), 51)
        self.assertEqual(parse_rank("101+"), 101)
        self.assertIsNone(parse_rank("not ranked"))

    def test_country_normalization_handles_qs_variants(self):
        self.assertEqual(norm_country("China (Mainland)"), "china")
        self.assertEqual(norm_country("Hong Kong SAR, China"), "hong kong")
        self.assertEqual(norm_country("英国"), "united kingdom")

    def test_vague_non_ascii_bucket_is_unknown(self):
        self.assertEqual(norm_country("其他亚洲国家"), "")


if __name__ == "__main__":
    unittest.main()
