from dataclasses import dataclass
from dotenv import load_dotenv
import os

load_dotenv()


@dataclass
class Settings:
    supabase_url: str
    supabase_service_key: str
    anthropic_api_key: str
    tavily_api_key: str
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
        anthropic_api_key=require("ANTHROPIC_API_KEY"),
        tavily_api_key=require("TAVILY_API_KEY"),
        batch_size=int(os.getenv("BATCH_SIZE", "10")),
    )
