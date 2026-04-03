from dataclasses import dataclass
from dotenv import load_dotenv
import os

load_dotenv()


@dataclass
class Settings:
    supabase_url: str
    supabase_service_key: str
    lark_app_id: str
    lark_app_secret: str
    lark_base_app_token: str
    lark_table_id: str
    anthropic_api_key: str
    batch_size: int


def load_settings() -> Settings:
    def require(key: str) -> str:
        val = os.getenv(key)
        if not val:
            raise EnvironmentError(f"Missing required env var: {key}")
        return val

    return Settings(
        supabase_url=require("SUPABASE_URL"),
        supabase_service_key=require("SUPABASE_SERVICE_KEY"),
        lark_app_id=require("LARK_APP_ID"),
        lark_app_secret=require("LARK_APP_SECRET"),
        lark_base_app_token=require("LARK_BASE_APP_TOKEN"),
        lark_table_id=require("LARK_TABLE_ID"),
        anthropic_api_key=require("ANTHROPIC_API_KEY"),
        batch_size=int(os.getenv("BATCH_SIZE", "10")),
    )
