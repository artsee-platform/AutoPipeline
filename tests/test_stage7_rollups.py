import unittest

from pipeline.stage7_school_comparison_rollups import (
    _career_signal,
    _has_strong_career_signal,
    _normalize_career_list,
    _rollup_payload_for_school,
)


class Stage7CareerRollupTests(unittest.TestCase):
    def test_normalizes_json_array_text(self):
        self.assertEqual(
            _normalize_career_list('["Designer", "Curator", "Art Director"]'),
            ["Designer", "Curator", "Art Director"],
        )

    def test_normalizes_plain_text_with_common_delimiters(self):
        self.assertEqual(
            _normalize_career_list("Designer; Curator、Art Director\nGallery manager"),
            ["Designer", "Curator", "Art Director", "Gallery manager"],
        )

    def test_keeps_single_prose_signal_instead_of_zeroing_it(self):
        self.assertEqual(
            _normalize_career_list(
                "Graduates pursue careers in design studios, galleries, museums, creative agencies."
            ),
            [
                "Graduates pursue careers in design studios",
                "galleries",
                "museums",
                "creative agencies.",
            ],
        )

    def test_career_signal_uses_radar_scale(self):
        self.assertEqual(_career_signal(0, 0), 1)
        self.assertEqual(_career_signal(1, 0), 2)
        self.assertEqual(_career_signal(3, 0), 3)
        self.assertEqual(_career_signal(6, 0), 4)
        self.assertEqual(_career_signal(10, 0), 5)
        self.assertEqual(_career_signal(1, 0, strong_signal=True), 5)

    def test_detects_strong_career_signal_words(self):
        self.assertTrue(_has_strong_career_signal("Strong industry links and internships"))
        self.assertTrue(_has_strong_career_signal(["Museums", "Galleries"]))
        self.assertFalse(_has_strong_career_signal("Independent practice"))

    def test_rollup_counts_text_backed_employment_fields(self):
        payload = _rollup_payload_for_school(
            school_id="school-1",
            program_ids=["program-1", "program-2"],
            career_entries=4,
            strong_career_signal=True,
            school_meta={
                "notable_alumni": '["Alice Example", "Bob Example"]',
                "international_students_page": "",
            },
            eval_rows=[],
            fee_rows=[],
            adm_rows=[],
        )

        self.assertEqual(payload["career_paths_total_entries"], 4)
        self.assertEqual(payload["notable_alumni_count"], 2)
        self.assertEqual(payload["career_signal_score"], 5)


if __name__ == "__main__":
    unittest.main()
