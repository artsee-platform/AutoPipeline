from typing import Optional
from typing_extensions import TypedDict


class School(TypedDict, total=False):
    id: Optional[int]
    name_zh: Optional[str]
    name_en: str                        # natural key — must be present
    country: Optional[str]
    city: Optional[str]
    school_type: Optional[str]
    qs_art_humanities_rank: Optional[int]
    qs_architecture_built_environment_rank: Optional[int]
    qs_art_design_rank: Optional[int]
    qs_history_of_art_rank: Optional[int]
    school_tier: Optional[int]
    official_website: Optional[str]
    international_students_page: Optional[str]
    founded_year: Optional[int]
    description: Optional[str]
    feature_tags: Optional[list]
    status: Optional[str]
    qs_overall_rank: Optional[int]
    entry_score_requirements: Optional[str]
    annual_intake: Optional[int]
    application_deadline: Optional[str]
    strength_disciplines: Optional[list]
    notable_alumni: Optional[list]
    logo_url: Optional[str]
    campus_image_urls: Optional[list]
