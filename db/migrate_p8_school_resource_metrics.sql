-- P8 — School-level resource metrics (separate from `schools` to keep the master row narrow).
--
-- Holds student–faculty signals, scholarships, facilities prose, optional structured extras.
-- Intended for comparison UI / radar "resources & experience" axis; pipelines can upsert here.
--
-- Prerequisites: `public.schools.id` is uuid (see migrate_programs_id_to_uuid.sql + fix_programs_school_id.sql).
-- Idempotent: safe to re-run via IF NOT EXISTS.

BEGIN;

CREATE TABLE IF NOT EXISTS public.school_resource_metrics (
  school_id uuid NOT NULL
    REFERENCES public.schools (id) ON UPDATE CASCADE ON DELETE CASCADE,

  -- Human-readable ratio when sources differ (e.g. "1:12", "~15:1"); use null when unknown.
  student_faculty_ratio_text text,

  -- 0–100 when a single percentage is evidenced; null otherwise.
  scholarship_ratio_pct numeric(5, 2)
    CONSTRAINT school_resource_metrics_schol_pct_chk
    CHECK (scholarship_ratio_pct IS NULL OR (scholarship_ratio_pct >= 0 AND scholarship_ratio_pct <= 100)),

  -- Short evidence-based prose for campuses / studios / labs (comparison table + tooltip).
  campus_facilities_summary text,

  -- Free-form notes (scraping caveats, country-specific definitions, etc.).
  resource_notes text,

  -- Audit fields mirroring satellite tables pattern.
  data_source text,
  source_url text,

  -- Optional: Claude / IPEDS blobs for re-processing without re-scraping.
  raw_evidence_json jsonb,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT school_resource_metrics_pkey PRIMARY KEY (school_id)
);

CREATE INDEX IF NOT EXISTS idx_school_resource_metrics_updated
  ON public.school_resource_metrics (updated_at DESC);

COMMENT ON TABLE public.school_resource_metrics IS
  'Student resources and campus-facing metrics keyed 1:1 to schools (kept out of schools row bloat).';

COMMENT ON COLUMN public.school_resource_metrics.student_faculty_ratio_text IS
  'Display/use as surfaced by the institution or official stats (format varies by country).';

COMMENT ON COLUMN public.school_resource_metrics.scholarship_ratio_pct IS
  'International or all-students aid share when evidenced as one percentage; else null.';

COMMENT ON COLUMN public.school_resource_metrics.campus_facilities_summary IS
  'Concise English or bilingual summary derived from verified sources — not marketing fluff from thin evidence.';

COMMIT;
