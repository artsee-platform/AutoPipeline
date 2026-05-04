-- Keep `schools.qs_overall_rank` as integer: use **NULL** when the institution has no
-- QS overall rank (not listed). Application layer maps NULL → display \"未上榜\".
-- Drops mistaken `qs_rank` if a prior draft migration added it.

BEGIN;

ALTER TABLE public.schools DROP COLUMN IF EXISTS qs_rank;

COMMENT ON COLUMN public.schools.qs_overall_rank IS
  'QS World University overall rank or NULL if not listed; see pipeline/qs_global_rank.display_qs_overall_rank';

COMMIT;
