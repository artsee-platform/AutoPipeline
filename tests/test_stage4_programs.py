import unittest
from unittest.mock import Mock, patch

from config.settings import Settings
from pipeline import stage4_programs


class Stage4ProgramTests(unittest.TestCase):
    def test_run_reads_raw_country_from_current_schema(self):
        settings = Settings(
            supabase_url="https://example.supabase.co",
            supabase_service_key="service-key",
            anthropic_api_key="anthropic-key",
            tavily_api_key="tavily-key",
            batch_size=1,
        )
        client = Mock()

        with (
            patch.object(stage4_programs, "_programs_school_id_is_uuid", return_value=True),
            patch.object(stage4_programs, "get_client", return_value=client),
            patch.object(stage4_programs.anthropic, "Anthropic", return_value=Mock()),
            patch.object(stage4_programs, "_fetch_all_schools", return_value=[]) as fetch_all,
        ):
            stage4_programs.run(settings, batch_size=1)

        fetch_all.assert_called_once_with(
            client,
            "id,name_en,name_zh,official_website,raw_country",
        )


if __name__ == "__main__":
    unittest.main()
