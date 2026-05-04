from typing import Optional
from typing_extensions import TypedDict


class School(TypedDict, total=False):
    id: Optional[int]
    name_zh: Optional[str]
    name_en: str                        # natural key — must be present
    raw_country: Optional[str]          # legacy mixed label (Chinese + buckets); was `country` pre-P2
    country_code: Optional[str]         # ISO 3166-1 alpha-2, FK → countries
    region_tag: Optional[str]           # FK → region_tags
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
    qs_overall_rank: Optional[int]  # NULL = not in QS overall table; show 未上榜 in UI
    entry_score_requirements: Optional[str]
    annual_intake: Optional[int]
    application_deadline: Optional[str]
    strength_disciplines: Optional[list]
    notable_alumni: Optional[list]
    logo_url: Optional[str]
    campus_image_urls: Optional[list]


class SchoolResourceMetrics(TypedDict, total=False):
    """1:1 with `school_resource_metrics`; `schools.id` is uuid (string from API)."""

    school_id: str
    student_faculty_ratio_text: Optional[str]
    scholarship_ratio_pct: Optional[float]
    campus_facilities_summary: Optional[str]
    resource_notes: Optional[str]
    data_source: Optional[str]
    source_url: Optional[str]
    raw_evidence_json: Optional[dict]


class SchoolComparisonRollup(TypedDict, total=False):
    """1:1 with `school_comparison_rollups`; built by Stage 7 from programs + satellites."""

    school_id: str
    median_application_difficulty_score: Optional[int]
    programs_with_evaluation_count: int
    median_international_tuition_fee: Optional[float]
    tuition_dominant_currency_code: Optional[str]
    international_fee_medians_json: Optional[dict]
    programs_with_international_fee_count: int
    intl_fee_mixed_currency: bool
    career_paths_total_entries: int
    notable_alumni_count: int
    career_signal_score: Optional[int]
    min_ielts_overall: Optional[float]
    min_toefl_ibt: Optional[int]
    programs_with_admissions_count: int
    has_international_students_page: Optional[bool]
    programs_active_for_rollup: int
    rollup_computed_at: Optional[str]
    updated_at: Optional[str]

