-- P0 normalization: lock down the small, stable controlled vocabularies on
-- public.programs and public.schools.
--
--   programs.status        : CHECK ('active','draft')
--   schools.status         : CHECK across the full pipeline state machine
--   schools.school_tier    : CHECK ('1','2','3')
--   schools.school_type    : FK -> school_types dictionary (bilingual labels)
--
-- Idempotent — safe to re-run. Wraps in a single transaction so partial
-- failures roll back cleanly.

BEGIN;

-- =============================================================
-- 1) programs.status  -- 2 values, simple CHECK
-- =============================================================
ALTER TABLE public.programs
  DROP CONSTRAINT IF EXISTS programs_status_chk;

ALTER TABLE public.programs
  ADD CONSTRAINT programs_status_chk
  CHECK (status IS NULL OR status IN ('active', 'draft'));

CREATE INDEX IF NOT EXISTS idx_programs_status
  ON public.programs (status);

-- =============================================================
-- 2) schools.status  -- pipeline state machine
--
-- Currently in DB: active / done / processing.
-- Pipeline code may also write: pending / enriched / qs_done / error
-- (see pipeline/stage{0,1,2,3}_*.py and db/supabase_client.py).
-- All seven are whitelisted so future pipeline runs don't trip the CHECK.
-- =============================================================
ALTER TABLE public.schools
  DROP CONSTRAINT IF EXISTS schools_status_chk;

ALTER TABLE public.schools
  ADD CONSTRAINT schools_status_chk
  CHECK (
    status IS NULL OR status IN (
      'pending', 'processing', 'enriched', 'qs_done',
      'active',  'done',       'error'
    )
  );

CREATE INDEX IF NOT EXISTS idx_schools_status
  ON public.schools (status);

-- =============================================================
-- 3) schools.school_tier  -- 3 values, integer-as-text
-- =============================================================
ALTER TABLE public.schools
  DROP CONSTRAINT IF EXISTS schools_school_tier_chk;

ALTER TABLE public.schools
  ADD CONSTRAINT schools_school_tier_chk
  CHECK (school_tier IS NULL OR school_tier IN ('1', '2', '3'));

CREATE INDEX IF NOT EXISTS idx_schools_school_tier
  ON public.schools (school_tier);

-- =============================================================
-- 4) schools.school_type  -- bilingual dictionary (7 codes)
-- =============================================================
CREATE TABLE IF NOT EXISTS public.school_types (
  code             text PRIMARY KEY,
  display_name     text NOT NULL,
  display_name_zh  text,
  sort_order       int  NOT NULL DEFAULT 0,
  is_active        boolean NOT NULL DEFAULT true,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

INSERT INTO public.school_types (code, display_name, display_name_zh, sort_order) VALUES
  ('art_academy',         'Art Academy',                  '艺术学院',         10),
  ('design_school',       'Design School',                '设计学院',         20),
  ('architecture_school', 'School of Architecture',       '建筑学院',         30),
  ('film_school',         'Film School',                  '电影学院',         40),
  ('performing_arts',     'Performing Arts School',       '表演艺术学院',     50),
  ('university_art_dept', 'University Art Department',    '综合大学艺术系',   60),
  ('multi_disciplinary',  'Multi-Disciplinary Institution', '综合性艺术院校', 70)
ON CONFLICT (code) DO UPDATE
  SET display_name    = EXCLUDED.display_name,
      display_name_zh = EXCLUDED.display_name_zh,
      sort_order      = EXCLUDED.sort_order,
      updated_at      = now();

-- Pre-flight: every value currently in schools.school_type must exist in the
-- dictionary, otherwise the FK creation below would fail and roll the whole
-- migration back. Surface offenders explicitly.
DO $$
DECLARE
  missing text;
BEGIN
  SELECT string_agg(DISTINCT s.school_type, ', ' ORDER BY s.school_type)
    INTO missing
  FROM public.schools s
  LEFT JOIN public.school_types st ON st.code = s.school_type
  WHERE s.school_type IS NOT NULL
    AND st.code IS NULL;

  IF missing IS NOT NULL THEN
    RAISE EXCEPTION
      'schools.school_type contains values not in school_types: %.  Add them to the seed INSERT above and re-run.',
      missing;
  END IF;
END$$;

ALTER TABLE public.schools
  DROP CONSTRAINT IF EXISTS schools_school_type_fkey;

ALTER TABLE public.schools
  ADD CONSTRAINT schools_school_type_fkey
  FOREIGN KEY (school_type)
  REFERENCES public.school_types (code)
  ON UPDATE CASCADE
  ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS idx_schools_school_type
  ON public.schools (school_type);

COMMIT;
