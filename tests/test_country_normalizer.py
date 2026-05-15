import unittest

from pipeline.country_normalizer import normalize_country_only, resolve_country


class CountryNormalizerTests(unittest.TestCase):
    def test_resolves_plain_country_labels(self):
        self.assertEqual(normalize_country_only("英国"), "GB")
        self.assertEqual(normalize_country_only("United States of America"), "US")
        self.assertEqual(resolve_country("中国"), {"country_code": "CN", "region_tag": None})

    def test_resolves_us_region_bucket_with_implied_country(self):
        self.assertEqual(
            resolve_country("加州旗舰"),
            {
                "country_code": "US",
                "region_tag": "us_california_flagship",
            },
        )

    def test_resolves_multi_country_bucket_without_country(self):
        self.assertEqual(
            resolve_country("北欧"),
            {
                "country_code": None,
                "region_tag": "nordics",
            },
        )


if __name__ == "__main__":
    unittest.main()
