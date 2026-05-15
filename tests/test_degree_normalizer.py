import unittest

from pipeline.degree_normalizer import normalize_degree


class DegreeNormalizerTests(unittest.TestCase):
    def test_normalizes_honours_bachelor_label(self):
        self.assertEqual(
            normalize_degree("BA (Hons)"),
            {
                "normalized_degree_type": "BA",
                "honours_flag": True,
            },
        )

    def test_normalizes_curated_combined_degree_alias(self):
        self.assertEqual(
            normalize_degree("BDesign/MArch"),
            {
                "normalized_degree_type": "BDes/MArch",
                "honours_flag": False,
            },
        )

    def test_unknown_combination_returns_null_for_review(self):
        self.assertEqual(
            normalize_degree("BA/MFA"),
            {
                "normalized_degree_type": None,
                "honours_flag": False,
            },
        )


if __name__ == "__main__":
    unittest.main()
