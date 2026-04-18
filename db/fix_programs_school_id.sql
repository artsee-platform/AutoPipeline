-- Align programs.school_id with public.schools.id (uuid).
-- Run once in Supabase SQL Editor before seeding programs.
-- Safe when programs is empty or you accept dropping orphan integer school_id values.

ALTER TABLE public.programs
  DROP CONSTRAINT IF EXISTS programs_school_id_fkey;

ALTER TABLE public.programs
  ALTER COLUMN school_id DROP NOT NULL;

ALTER TABLE public.programs
  ALTER COLUMN school_id TYPE uuid USING (NULL::uuid);

ALTER TABLE public.programs
  ALTER COLUMN school_id SET NOT NULL;

ALTER TABLE public.programs
  ADD CONSTRAINT programs_school_id_fkey
  FOREIGN KEY (school_id) REFERENCES public.schools(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_programs_school_id ON public.programs (school_id);
