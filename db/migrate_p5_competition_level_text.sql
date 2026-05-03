-- Widen `program_evaluations.competition_level` for 2–3 sentence prose.
--
-- Stage 5 used to cap this field at 50 characters in Python and in the LLM prompt,
-- which cut off mid-sentence. The column is widened to unbounded `text`.
--
-- Idempotent on PostgreSQL (varchar(n) → text is always safe).

BEGIN;

ALTER TABLE public.program_evaluations
  ALTER COLUMN competition_level TYPE text;

COMMENT ON COLUMN public.program_evaluations.competition_level IS
  'Evidence-based 2–3 sentence summary of how competitive admissions appear; populated by stage5_program_satellite.py';

COMMIT;
