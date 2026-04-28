-- P4: `program_evaluations.application_difficulty_score` → smallint 1–5 (+ CHECK)
--
-- Legacy values were free-text labels (Moderate, 4.5/5, …). This migration maps
-- them with the same intent as `pipeline/evaluation_difficulty.py`.
-- Rows that cannot be mapped become NULL — review with:
--   SELECT id, program_id, application_difficulty_score  -- before migration, copy if needed
--
-- After applying, Stage 5 prompts + inserts must use integers only (see pipeline).
--
-- Pre-req: column is castable to text (varchar/text). If type already smallint, skip this file.

BEGIN;

ALTER TABLE public.program_evaluations
  DROP CONSTRAINT IF EXISTS program_evaluations_application_difficulty_score_chk;

ALTER TABLE public.program_evaluations
  ALTER COLUMN application_difficulty_score DROP DEFAULT;

ALTER TABLE public.program_evaluations
  ALTER COLUMN application_difficulty_score TYPE smallint
  USING (
    CAST(
      CASE
        WHEN application_difficulty_score IS NULL
          OR trim(application_difficulty_score::text) = ''
          THEN NULL::integer

        WHEN application_difficulty_score::text ~ '^[0-9]+(\.[0-9]+)?[[:space:]]*/[[:space:]]*5[[:space:]]*$'
          THEN LEAST(
            5,
            GREATEST(
              1,
              floor(
                CAST(
                  substring(
                    application_difficulty_score::text
                    FROM '^([0-9]+(\.[0-9]+)?)'
                  ) AS numeric
                ) + 0.5
              )::integer
            )
          )

        WHEN application_difficulty_score::text ~ '^[0-9]+$'
          THEN LEAST(
            5,
            GREATEST(1, application_difficulty_score::text::integer)
          )

        WHEN lower(application_difficulty_score::text) LIKE '%very low%'
          THEN 1

        WHEN lower(application_difficulty_score::text) LIKE '%very high%'
          OR lower(application_difficulty_score::text) LIKE '%extremely high%'
          THEN 5

        WHEN lower(application_difficulty_score::text) LIKE '%low-moderate%'
          OR lower(application_difficulty_score::text) LIKE '%low moderate%'
          OR lower(application_difficulty_score::text) LIKE '%low-medium%'
          OR lower(application_difficulty_score::text) LIKE '%low medium%'
          OR lower(application_difficulty_score::text) LIKE '%low selectivity%'
          THEN 2

        WHEN lower(application_difficulty_score::text) LIKE '%moderately high%'
          OR lower(application_difficulty_score::text) LIKE '%moderate-high%'
          OR lower(application_difficulty_score::text) LIKE '%moderate high%'
          THEN 4

        WHEN lower(application_difficulty_score::text) LIKE '%selective%'
          THEN 4

        WHEN lower(application_difficulty_score::text) LIKE '%competitive%'
          THEN 3

        WHEN lower(application_difficulty_score::text) LIKE '%moderate%'
          THEN 3

        WHEN lower(application_difficulty_score::text) LIKE '%high%'
          THEN 4

        WHEN lower(application_difficulty_score::text) LIKE '%low%'
          THEN 2

        ELSE NULL::integer
      END
    AS smallint)
  );

ALTER TABLE public.program_evaluations
  ADD CONSTRAINT program_evaluations_application_difficulty_score_chk
  CHECK (application_difficulty_score IS NULL OR (application_difficulty_score BETWEEN 1 AND 5));

COMMENT ON COLUMN public.program_evaluations.application_difficulty_score IS
  'Subjective admission difficulty 1 (easiest) – 5 (hardest). JSON + pipeline use integers only.';

COMMIT;
