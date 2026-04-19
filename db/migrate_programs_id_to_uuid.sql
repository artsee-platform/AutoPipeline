-- Migrate public.programs.id from integer/serial to uuid, and repoint all FKs.
-- Child tables (from your error): program_admissions, program_fees, program_evaluations,
-- program_art_categories, user_favorites, application_tracker — all reference programs(id).
--
-- Backup first. Run in Supabase SQL Editor as a single transaction.

BEGIN;

-- 1) Stable uuid map on programs (one new uuid per row)
ALTER TABLE public.programs ADD COLUMN IF NOT EXISTS id_new uuid;
UPDATE public.programs SET id_new = gen_random_uuid() WHERE id_new IS NULL;
ALTER TABLE public.programs ALTER COLUMN id_new SET NOT NULL;

-- 2) Shadow uuid FK columns on children (backfill from map)
ALTER TABLE public.program_admissions ADD COLUMN IF NOT EXISTS program_id_new uuid;
UPDATE public.program_admissions pa SET program_id_new = p.id_new FROM public.programs p WHERE pa.program_id = p.id;

ALTER TABLE public.program_fees ADD COLUMN IF NOT EXISTS program_id_new uuid;
UPDATE public.program_fees pf SET program_id_new = p.id_new FROM public.programs p WHERE pf.program_id = p.id;

ALTER TABLE public.program_evaluations ADD COLUMN IF NOT EXISTS program_id_new uuid;
UPDATE public.program_evaluations pe SET program_id_new = p.id_new FROM public.programs p WHERE pe.program_id = p.id;

ALTER TABLE public.program_art_categories ADD COLUMN IF NOT EXISTS program_id_new uuid;
UPDATE public.program_art_categories pac SET program_id_new = p.id_new FROM public.programs p WHERE pac.program_id = p.id;

ALTER TABLE public.user_favorites ADD COLUMN IF NOT EXISTS program_id_new uuid;
UPDATE public.user_favorites uf SET program_id_new = p.id_new FROM public.programs p WHERE uf.program_id = p.id;

ALTER TABLE public.application_tracker ADD COLUMN IF NOT EXISTS program_id_new uuid;
UPDATE public.application_tracker at SET program_id_new = p.id_new FROM public.programs p
WHERE at.program_id IS NOT NULL AND at.program_id = p.id;

-- 3) Drop FKs that depend on programs_pkey (order does not matter)
ALTER TABLE public.program_admissions DROP CONSTRAINT IF EXISTS program_admissions_program_id_fkey;
ALTER TABLE public.program_fees DROP CONSTRAINT IF EXISTS program_fees_program_id_fkey;
ALTER TABLE public.program_evaluations DROP CONSTRAINT IF EXISTS program_evaluations_program_id_fkey;
ALTER TABLE public.program_art_categories DROP CONSTRAINT IF EXISTS program_art_categories_program_id_fkey;
ALTER TABLE public.user_favorites DROP CONSTRAINT IF EXISTS user_favorites_program_id_fkey;
ALTER TABLE public.application_tracker DROP CONSTRAINT IF EXISTS application_tracker_program_id_fkey;

-- 4) Replace child program_id (int) with uuid column
ALTER TABLE public.program_admissions DROP COLUMN program_id;
ALTER TABLE public.program_admissions RENAME COLUMN program_id_new TO program_id;
ALTER TABLE public.program_admissions ALTER COLUMN program_id SET NOT NULL;

ALTER TABLE public.program_fees DROP COLUMN program_id;
ALTER TABLE public.program_fees RENAME COLUMN program_id_new TO program_id;
ALTER TABLE public.program_fees ALTER COLUMN program_id SET NOT NULL;

ALTER TABLE public.program_evaluations DROP COLUMN program_id;
ALTER TABLE public.program_evaluations RENAME COLUMN program_id_new TO program_id;
ALTER TABLE public.program_evaluations ALTER COLUMN program_id SET NOT NULL;

ALTER TABLE public.program_art_categories DROP COLUMN program_id;
ALTER TABLE public.program_art_categories RENAME COLUMN program_id_new TO program_id;
ALTER TABLE public.program_art_categories ALTER COLUMN program_id SET NOT NULL;

ALTER TABLE public.user_favorites DROP COLUMN program_id;
ALTER TABLE public.user_favorites RENAME COLUMN program_id_new TO program_id;
ALTER TABLE public.user_favorites ALTER COLUMN program_id SET NOT NULL;

ALTER TABLE public.application_tracker DROP COLUMN program_id;
ALTER TABLE public.application_tracker RENAME COLUMN program_id_new TO program_id;
-- keep nullable if you had nullable application_tracker.program_id

-- 5) Swap programs primary key to uuid
ALTER TABLE public.programs DROP CONSTRAINT programs_pkey;
ALTER TABLE public.programs DROP COLUMN id;
ALTER TABLE public.programs RENAME COLUMN id_new TO id;
ALTER TABLE public.programs ADD CONSTRAINT programs_pkey PRIMARY KEY (id);
ALTER TABLE public.programs ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- 6) Recreate FKs (adjust ON DELETE to match your previous definitions if needed)
ALTER TABLE public.program_admissions
  ADD CONSTRAINT program_admissions_program_id_fkey
  FOREIGN KEY (program_id) REFERENCES public.programs (id) ON DELETE CASCADE;

ALTER TABLE public.program_fees
  ADD CONSTRAINT program_fees_program_id_fkey
  FOREIGN KEY (program_id) REFERENCES public.programs (id) ON DELETE CASCADE;

ALTER TABLE public.program_evaluations
  ADD CONSTRAINT program_evaluations_program_id_fkey
  FOREIGN KEY (program_id) REFERENCES public.programs (id) ON DELETE CASCADE;

ALTER TABLE public.program_art_categories
  ADD CONSTRAINT program_art_categories_program_id_fkey
  FOREIGN KEY (program_id) REFERENCES public.programs (id) ON DELETE CASCADE;

ALTER TABLE public.user_favorites
  ADD CONSTRAINT user_favorites_program_id_fkey
  FOREIGN KEY (program_id) REFERENCES public.programs (id) ON DELETE CASCADE;

ALTER TABLE public.application_tracker
  ADD CONSTRAINT application_tracker_program_id_fkey
  FOREIGN KEY (program_id) REFERENCES public.programs (id) ON DELETE SET NULL;

COMMIT;
