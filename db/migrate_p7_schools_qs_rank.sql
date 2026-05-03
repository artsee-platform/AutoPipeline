-- `schools.qs_overall_rank`: store QS overall rank as text (digits or 未上榜).
-- Drops mistaken `qs_rank` if a prior draft migration added it.

BEGIN;

ALTER TABLE public.schools DROP COLUMN IF EXISTS qs_rank;

ALTER TABLE public.schools
  ALTER COLUMN qs_overall_rank TYPE text
  USING (
    CASE
      WHEN qs_overall_rank IS NULL THEN NULL
      -- Supports existing integer column or already-text values
      ELSE trim(qs_overall_rank::text)
    END
  );

COMMENT ON COLUMN public.schools.qs_overall_rank IS
  'QS World University overall rank: numeric string or 未上榜; see pipeline/qs_global_rank.py';

COMMIT;
