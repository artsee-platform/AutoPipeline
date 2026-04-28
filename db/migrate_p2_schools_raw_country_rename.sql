-- P2 cleanup: clarify legacy geography text on `schools`.
--
-- Historical column: `country` held mixed semantics (real country Chinese labels
-- + product buckets like "加州旗舰"). After P1, canonical geography is
-- `country_code` (FK → countries) and `region_tag` (FK → region_tags).
--
-- This migration only renames the legacy column for audit / migration tracing:
--     country  →  raw_country
--
-- It does NOT drop data. If `country` was already removed and `raw_country`
-- never existed, an empty nullable `raw_country` column is added so Stage 0
-- can still write the Excel label.
--
-- Optional NOT NULL on `country_code` is intentionally NOT applied here — run
-- only after verifying zero NULLs:
--     SELECT count(*) FROM public.schools WHERE country_code IS NULL;
--
-- Idempotent.

BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'schools'
      AND column_name  = 'country'
  ) AND NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'schools'
      AND column_name  = 'raw_country'
  ) THEN
    ALTER TABLE public.schools RENAME COLUMN country TO raw_country;

  ELSIF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'schools'
      AND column_name  = 'raw_country'
  ) THEN
    -- `country` was already dropped elsewhere; keep a nullable audit column for seeds.
    ALTER TABLE public.schools ADD COLUMN IF NOT EXISTS raw_country text;
  END IF;
END$$;

COMMIT;

-- Optional — only when every row has a code:
--   SELECT count(*) FROM public.schools WHERE country_code IS NULL;
-- should return 0.
--
-- ALTER TABLE public.schools ALTER COLUMN country_code SET NOT NULL;
