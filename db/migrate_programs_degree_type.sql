-- Redesign programs.degree_type into a three-layer scheme + two flags.
--
--   raw_degree_type          : original free text (renamed from degree_type)
--   normalized_degree_type   : canonical short label, e.g. "BDes", "PhD", "BA", "Master"
--   degree_family            : coarse bucket — Bachelor / Master / Doctorate / Diploma / Other
--   honours_flag             : true when the raw text carries a (Hons)/(Honours) marker
--   combined_degree_flag     : true for joint/double degrees such as "BA/MArch"
--   combined_with            : canonical parts of a combined degree, e.g. {"BA","MArch"}
--
-- This migration is schema-only. The existing raw values are preserved as-is in
-- raw_degree_type; backfill of the five new columns is performed by
-- scripts/backfill_degree_normalization.py using pipeline/degree_normalizer.py.
-- Run this SQL first, then the Python backfill.
--
-- Idempotent: safe to re-run. Wrap in a transaction so failures roll back cleanly.

BEGIN;

-- 1) Preserve original text. Only rename if degree_type still exists and raw_degree_type does not.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'programs'
      AND column_name  = 'degree_type'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'programs'
      AND column_name  = 'raw_degree_type'
  ) THEN
    EXECUTE 'ALTER TABLE public.programs RENAME COLUMN degree_type TO raw_degree_type';
  END IF;
END$$;

-- 2) Add new columns (idempotent)
ALTER TABLE public.programs
  ADD COLUMN IF NOT EXISTS normalized_degree_type text;

ALTER TABLE public.programs
  ADD COLUMN IF NOT EXISTS degree_family text;

ALTER TABLE public.programs
  ADD COLUMN IF NOT EXISTS honours_flag boolean NOT NULL DEFAULT false;

ALTER TABLE public.programs
  ADD COLUMN IF NOT EXISTS combined_degree_flag boolean NOT NULL DEFAULT false;

ALTER TABLE public.programs
  ADD COLUMN IF NOT EXISTS combined_with text[];

-- 3) Constrain degree_family to the five buckets the product uses
ALTER TABLE public.programs
  DROP CONSTRAINT IF EXISTS programs_degree_family_chk;

ALTER TABLE public.programs
  ADD CONSTRAINT programs_degree_family_chk
  CHECK (
    degree_family IS NULL
    OR degree_family IN ('Bachelor', 'Master', 'Doctorate', 'Diploma', 'Other')
  );

-- 4) Indexes for the filters the UI will run
CREATE INDEX IF NOT EXISTS idx_programs_degree_family
  ON public.programs (degree_family);

CREATE INDEX IF NOT EXISTS idx_programs_normalized_degree_type
  ON public.programs (normalized_degree_type);

COMMIT;
