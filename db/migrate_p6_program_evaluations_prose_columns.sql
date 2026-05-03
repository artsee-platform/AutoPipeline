-- Widen evaluation prose columns so nothing is cut off by varchar limits.
--
-- Stage 5 no longer truncates competition_level, data_source, etc. in Python.
-- competition_level is already `text` after migrate_p5_competition_level_text.sql;
-- this migration ensures data_source / evidence_note / source_url can grow with the LLM output.
--
-- Idempotent: varchar(n) → text is always safe in PostgreSQL.

BEGIN;

ALTER TABLE public.program_evaluations
  ALTER COLUMN data_source TYPE text,
  ALTER COLUMN evidence_note TYPE text,
  ALTER COLUMN source_url TYPE text;

COMMIT;
