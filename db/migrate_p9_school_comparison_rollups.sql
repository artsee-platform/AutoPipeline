-- P9 — Per-school rollup metrics for comparison / radar axes built from programs + satellites.
--
-- Raw inputs:
--   * Dim 1 「学术与声誉」: qs_* ranks + school_tier on `public.schools` (Stages 2/0–2)
--   * Dim 「申请难度」: program_evaluations (Stage 5)
--   * Dim 「费用负担」: program_fees (Stage 5)
--   * Dim 「国际化与语言门槛」: program_admissions + schools.international_students_page
--   * Dim 「职业衔接」: programs.career_paths + schools.notable_alumni
--   * Dim 「资源与体验」: school_resource_metrics (Stage 6)
--
-- This table aggregates program-level payloads so the API/joins stay simple vs per-request SQL.
--
-- Idempotent via IF NOT EXISTS.

BEGIN;

CREATE TABLE IF NOT EXISTS public.school_comparison_rollups (
  school_id uuid NOT NULL
    REFERENCES public.schools (id) ON UPDATE CASCADE ON DELETE CASCADE,

  median_application_difficulty_score smallint
    CONSTRAINT scr_median_diff_chk
    CHECK (median_application_difficulty_score IS NULL OR (median_application_difficulty_score BETWEEN 1 AND 5)),
  programs_with_evaluation_count integer NOT NULL DEFAULT 0,

  median_international_tuition_fee numeric,
  tuition_dominant_currency_code text,
  international_fee_medians_json jsonb
    CONSTRAINT scr_fee_med_json_chk CHECK (international_fee_medians_json IS NULL OR jsonb_typeof(international_fee_medians_json) = 'object'),
  programs_with_international_fee_count integer NOT NULL DEFAULT 0,
  intl_fee_mixed_currency boolean NOT NULL DEFAULT false,

  career_paths_total_entries integer NOT NULL DEFAULT 0,
  notable_alumni_count integer NOT NULL DEFAULT 0,
  career_signal_score smallint
    CONSTRAINT scr_career_signal_chk CHECK (career_signal_score IS NULL OR (career_signal_score BETWEEN 1 AND 5)),

  min_ielts_overall numeric
    CONSTRAINT scr_min_ielts_chk CHECK (min_ielts_overall IS NULL OR (min_ielts_overall >= 0 AND min_ielts_overall <= 9)),
  min_toefl_ibt integer,
  programs_with_admissions_count integer NOT NULL DEFAULT 0,
  has_international_students_page boolean,

  programs_active_for_rollup integer NOT NULL DEFAULT 0,

  rollup_computed_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT school_comparison_rollups_pkey PRIMARY KEY (school_id)
);

CREATE INDEX IF NOT EXISTS idx_school_comparison_rollups_updated
  ON public.school_comparison_rollups (updated_at DESC);

COMMENT ON TABLE public.school_comparison_rollups IS
  'Aggregated program/satellite KPIs keyed 1:1 to schools — computed by Stage 7 Python job.';

COMMENT ON COLUMN public.school_comparison_rollups.international_fee_medians_json IS
  'Per-currency median international tuition, e.g. {"GBP":22000,"USD":38500}; use when intl_fee_mixed_currency or for charts.';

COMMIT;
