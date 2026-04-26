-- Promote `programs.normalized_degree_type` to a controlled vocabulary backed
-- by a dictionary table, and retire the three columns whose information is
-- now derivable from that dictionary.
--
-- Final shape of programs (degree-related slice only):
--   raw_degree_type           : text (free text, audit trail; unchanged)
--   normalized_degree_type    : text → FK degree_labels.code (controlled)
--   honours_flag              : boolean (unchanged)
--
-- Dropped:
--   degree_family             : derive via JOIN degree_labels
--   combined_degree_flag      : same
--   combined_with             : same (now degree_labels.parts)
--
-- Pre-condition: db/migrate_programs_degree_type.sql has already been applied.
-- Pre-condition: scripts/backfill_degree_normalization.py has been re-run with
-- the latest normalizer (so all `programs.normalized_degree_type` values are
-- known controlled-vocabulary codes — otherwise the FK creation step fails).
--
-- Idempotent: safe to re-run.

BEGIN;

-- 1) Dictionary table for the controlled vocabulary.
CREATE TABLE IF NOT EXISTS public.degree_labels (
  code             text PRIMARY KEY,
  display_name     text,                        -- shown in UI; defaults to code
  display_name_zh  text,                        -- Chinese label, optional
  family           text NOT NULL,
  is_combined      boolean NOT NULL DEFAULT false,
  parts            text[],                      -- component codes for combined entries
  sort_order       int NOT NULL DEFAULT 0,
  is_active        boolean NOT NULL DEFAULT true,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT degree_labels_family_chk
    CHECK (family IN ('Bachelor','Master','Doctorate','Diploma','Other')),
  CONSTRAINT degree_labels_combined_chk
    CHECK (
      (is_combined = false AND parts IS NULL)
      OR (is_combined = true  AND array_length(parts, 1) >= 2)
    )
);

CREATE INDEX IF NOT EXISTS idx_degree_labels_family
  ON public.degree_labels (family);
CREATE INDEX IF NOT EXISTS idx_degree_labels_active_sort
  ON public.degree_labels (is_active, sort_order);

-- 2) Seed the controlled vocabulary. Snapshot generated from
-- pipeline/degree_normalizer.py via iter_label_catalog(). To pick up new
-- entries after editing the normalizer, run:
--     python -m scripts.sync_degree_labels
INSERT INTO public.degree_labels (code, family, is_combined, parts) VALUES
  ('BA', 'Bachelor', false, NULL),
  ('BSc', 'Bachelor', false, NULL),
  ('BS', 'Bachelor', false, NULL),
  ('BFA', 'Bachelor', false, NULL),
  ('BDes', 'Bachelor', false, NULL),
  ('BEng', 'Bachelor', false, NULL),
  ('BArch', 'Bachelor', false, NULL),
  ('BMus', 'Bachelor', false, NULL),
  ('BBA', 'Bachelor', false, NULL),
  ('LLB', 'Bachelor', false, NULL),
  ('BEd', 'Bachelor', false, NULL),
  ('BEnvD', 'Bachelor', false, NULL),
  ('BAS', 'Bachelor', false, NULL),
  ('BAFT', 'Bachelor', false, NULL),
  ('BVA', 'Bachelor', false, NULL),
  ('BDI', 'Bachelor', false, NULL),
  ('BID', 'Bachelor', false, NULL),
  ('Bachelor', 'Bachelor', false, NULL),
  ('Licenciatura', 'Bachelor', false, NULL),
  ('Specialist', 'Bachelor', false, NULL),
  ('MA', 'Master', false, NULL),
  ('MSc', 'Master', false, NULL),
  ('MS', 'Master', false, NULL),
  ('MFA', 'Master', false, NULL),
  ('MDes', 'Master', false, NULL),
  ('MArch', 'Master', false, NULL),
  ('MEng', 'Master', false, NULL),
  ('MPhil', 'Master', false, NULL),
  ('MBA', 'Master', false, NULL),
  ('MRes', 'Master', false, NULL),
  ('MMus', 'Master', false, NULL),
  ('LLM', 'Master', false, NULL),
  ('MLitt', 'Master', false, NULL),
  ('MPA', 'Master', false, NULL),
  ('MVS', 'Master', false, NULL),
  ('MDI', 'Master', false, NULL),
  ('MID', 'Master', false, NULL),
  ('Meisterschüler', 'Master', false, NULL),
  ('Master', 'Master', false, NULL),
  ('PhD', 'Doctorate', false, NULL),
  ('DPhil', 'Doctorate', false, NULL),
  ('EdD', 'Doctorate', false, NULL),
  ('MD', 'Doctorate', false, NULL),
  ('DFA', 'Doctorate', false, NULL),
  ('Doctorate', 'Doctorate', false, NULL),
  ('Diploma', 'Diploma', false, NULL),
  ('Higher Diploma', 'Diploma', false, NULL),
  ('HND', 'Diploma', false, NULL),
  ('Certificate', 'Diploma', false, NULL),
  ('PGDip', 'Diploma', false, NULL),
  ('PGCert', 'Diploma', false, NULL),
  ('Foundation', 'Diploma', false, NULL),
  ('AFA', 'Diploma', false, NULL),
  ('BA/BS',      'Bachelor', true, ARRAY['BA','BS']),
  ('BA/MA',      'Master',   true, ARRAY['BA','MA']),
  ('BA/MArch',   'Master',   true, ARRAY['BA','MArch']),
  ('BA/MDes',    'Master',   true, ARRAY['BA','MDes']),
  ('BDes/MArch', 'Master',   true, ARRAY['BDes','MArch']),
  ('MDes/MFA',   'Master',   true, ARRAY['MDes','MFA'])
ON CONFLICT (code) DO UPDATE
  SET family      = EXCLUDED.family,
      is_combined = EXCLUDED.is_combined,
      parts       = EXCLUDED.parts,
      updated_at  = now();

-- 3) Pre-flight check: every value currently in programs.normalized_degree_type
-- must already exist in degree_labels, otherwise the FK below would fail and
-- the whole transaction would abort. Surface the offenders explicitly.
DO $$
DECLARE
  missing text;
BEGIN
  SELECT string_agg(DISTINCT p.normalized_degree_type, ', ' ORDER BY p.normalized_degree_type)
    INTO missing
  FROM public.programs p
  LEFT JOIN public.degree_labels d ON d.code = p.normalized_degree_type
  WHERE p.normalized_degree_type IS NOT NULL
    AND d.code IS NULL;

  IF missing IS NOT NULL THEN
    RAISE EXCEPTION
      'programs.normalized_degree_type contains values not in degree_labels: %.  Re-run scripts/backfill_degree_normalization.py with the latest normalizer, or add the missing codes to pipeline/degree_normalizer.py and resync.',
      missing;
  END IF;
END$$;

-- 4) FK constraint. Use NOT VALID + VALIDATE so existing rows are checked
-- against the dictionary, and future inserts are constrained.
ALTER TABLE public.programs
  DROP CONSTRAINT IF EXISTS programs_normalized_degree_type_fkey;

ALTER TABLE public.programs
  ADD CONSTRAINT programs_normalized_degree_type_fkey
  FOREIGN KEY (normalized_degree_type)
  REFERENCES public.degree_labels (code)
  ON UPDATE CASCADE
  ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS idx_programs_normalized_degree_type
  ON public.programs (normalized_degree_type);

-- 5) Drop the now-redundant columns. Their information is fully recoverable by
-- joining degree_labels on normalized_degree_type, so removing them prevents
-- future drift between the columns and the dictionary.
ALTER TABLE public.programs
  DROP COLUMN IF EXISTS degree_family;
ALTER TABLE public.programs
  DROP COLUMN IF EXISTS combined_degree_flag;
ALTER TABLE public.programs
  DROP COLUMN IF EXISTS combined_with;

COMMIT;

-- Optional follow-up: drop the standalone CHECK that used to live on
-- degree_family — it lives on degree_labels now.
ALTER TABLE public.programs
  DROP CONSTRAINT IF EXISTS programs_degree_family_chk;
