-- Low-cardinality text-column discovery — run in Supabase SQL Editor.
--
-- Purpose
--   Surface candidate columns for CHECK / dictionary-table / FK normalization
--   (rough stats-based + optional exact passes).
--
-- Tuning
--   @max_distinct_approx : pg_stats positive n_distinct upper bound (estimator).
--   @max_distinct_exact  : exact DISTINCT count ceiling in DO block (slower).
--
-- Steps (in file order)
--   §0  optional ANALYZE;
--   §1  Fast triage — pg_stats SELECT
--   §2  Inventory — text/varchar columns, then array columns, then json/jsonb columns
--   §3  Exact low-cardinality — DO $$ … $$ (NOTICE only; Results tab empty in Supabase)
--   §3b CREATE FUNCTION … + SELECT (use this for a real result grid; drops not auto)
--   §4  Commented program sanity queries; uncomment as needed

-- ---------------------------------------------------------------------------
-- 0) Refresh planner stats (optional but helps pg_stats accuracy)
-- ---------------------------------------------------------------------------
-- ANALYZE;

-- ---------------------------------------------------------------------------
-- 1) Fast triage — pg_stats (PostgreSQL estimator)
--
--   n_distinct > 0  → estimated COUNT(DISTINCT) for that column
--   n_distinct < 0  → NOT used here (fraction form; high-cardinality signal)
--
--   Also lists most_common_vals for quick eyeballing of typos / synonyms.
-- ---------------------------------------------------------------------------
SELECT
  s.schemaname,
  s.tablename      AS table_name,
  s.attname        AS column_name,
  s.n_distinct     AS approx_distinct_nonnull,
  s.null_frac,
  s.most_common_vals::text AS most_common_vals,
  s.most_common_freqs
FROM pg_stats s
WHERE s.schemaname = 'public'
  AND s.n_distinct > 0
  AND s.n_distinct <= 40   -- raise to 50–80 if you want a wider net
ORDER BY s.tablename, s.n_distinct NULLS LAST, s.attname;

-- ---------------------------------------------------------------------------
-- 2) Inventory — all scalar text-like columns (exact types, no counts)
-- ---------------------------------------------------------------------------
SELECT
  table_name,
  column_name,
  data_type,
  udt_name,
  is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND data_type IN ('text', 'character varying')
ORDER BY table_name, ordinal_position;

-- Arrays of text (if any column still uses text[] / varchar[])
SELECT
  table_name,
  column_name,
  data_type,
  udt_name
FROM information_schema.columns
WHERE table_schema = 'public'
  AND data_type = 'ARRAY'
  AND udt_name IN ('_text', '_varchar')
ORDER BY table_name, ordinal_position;

-- JSON / JSONB columns (distinct-element analysis needs jsonb_array_elements_text, etc.)
SELECT
  table_name,
  column_name,
  data_type,
  udt_name
FROM information_schema.columns
WHERE table_schema = 'public'
  AND data_type IN ('json', 'jsonb')
ORDER BY table_name, ordinal_position;

-- ---------------------------------------------------------------------------
-- 3) Exact low-cardinality pass — scalar text/varchar ONLY
--
--   WARNING: scans each qualifying column once. Fine for small DBs;
--   comment out or narrow tables if runtime is too long.
--
--   This block only emits PostgreSQL NOTICE — it does NOT return rows. In
--   Supabase SQL Editor the "Results" tab stays empty; notices may appear under
--   **Messages** / server log (easy to miss). Prefer §3b for a real result grid.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
  r       record;
  n       bigint;
  lim     int := 30;  -- only report columns with 1..lim distinct non-null values
BEGIN
  FOR r IN
    SELECT c.table_schema, c.table_name, c.column_name
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema
     AND t.table_name   = c.table_name
     AND t.table_type   = 'BASE TABLE'
    WHERE c.table_schema = 'public'
      AND c.data_type IN ('text', 'character varying')
      -- Optional: restrict to tables you care about:
      -- AND c.table_name IN ('programs', 'schools', 'program_admissions', 'program_fees')
    ORDER BY c.table_name, c.column_name
  LOOP
    EXECUTE format(
      'SELECT count(*) FROM (
         SELECT DISTINCT %I FROM %I.%I AS t WHERE %I IS NOT NULL
       ) AS x',
      r.column_name,
      r.table_schema,
      r.table_name,
      r.column_name
    )
    INTO n;

    IF n >= 1 AND n <= lim THEN
      RAISE NOTICE '%.% | distinct_non_null = %',
        r.table_name, r.column_name, n;
    END IF;
  END LOOP;
END$$;

-- ---------------------------------------------------------------------------
-- 3b) Same logic as §3, but RETURNS ROWS for the Supabase "Results" tab
--
--   Tuning: change 30 to widen/narrow the distinct-count ceiling.
--
--   Optional cleanup after you finish reading the grid:
--   DROP FUNCTION IF EXISTS public._diagnostic_low_cardinality_exact(integer);
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public._diagnostic_low_cardinality_exact(p_lim int DEFAULT 30)
RETURNS TABLE(tbl text, col text, distinct_nonnull bigint)
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = public
AS $fn$
DECLARE
  r   record;
  n   bigint;
BEGIN
  FOR r IN
    SELECT c.table_schema AS sch, c.table_name AS tn, c.column_name AS cn
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema
     AND t.table_name   = c.table_name
     AND t.table_type   = 'BASE TABLE'
    WHERE c.table_schema = 'public'
      AND c.data_type IN ('text', 'character varying')
    ORDER BY c.table_name, c.column_name
  LOOP
    EXECUTE format(
      'SELECT count(*) FROM (
         SELECT DISTINCT %I FROM %I.%I AS x WHERE %I IS NOT NULL
       ) AS c',
      r.cn, r.sch, r.tn, r.cn
    )
    INTO n;

    IF n >= 1 AND n <= p_lim THEN
      tbl := r.tn;
      col := r.cn;
      distinct_nonnull := n;
      RETURN NEXT;
    END IF;
  END LOOP;
  RETURN;
END;
$fn$;

SELECT * FROM public._diagnostic_low_cardinality_exact(30)
ORDER BY tbl, col;

-- ---------------------------------------------------------------------------
-- 4) Program field sanity (adjust table name if yours differs)
--
--   Pipeline intent (see pipeline/stage4_programs.py):
--     duration_months  : integer calendar months (optional CHECK in DB — you skipped for now)
--     intake_months    : often jsonb array of month LABELS, e.g. ["September","January"]
--                        — NOT an int; normalize elements if you need filters
--     study_mode       : short English token from the LLM — good candidate for CHECK
--                        after fixing the two composite strings (see product rule)
-- ---------------------------------------------------------------------------
-- SELECT pg_typeof(duration_months) AS duration_type,
--        pg_typeof(intake_months) AS intake_type,
--        pg_typeof(study_mode)   AS study_mode_type
-- FROM public.programs
-- LIMIT 1;

-- SELECT 'study_mode' AS col, study_mode AS val, count(*) AS n
-- FROM public.programs
-- WHERE study_mode IS NOT NULL
-- GROUP BY 1, 2
-- ORDER BY n DESC;

-- SELECT 'minimum_education' AS col, minimum_education AS val, count(*) AS n
-- FROM public.programs
-- WHERE minimum_education IS NOT NULL
-- GROUP BY 1, 2
-- ORDER BY n DESC;

If intake_months is jsonb storing a JSON array of strings:
SELECT elem AS month_token, count(*) AS n
FROM (
  SELECT jsonb_array_elements_text(
    CASE WHEN jsonb_typeof(intake_months) = 'array' THEN intake_months ELSE '[]'::jsonb END
  ) AS elem
  FROM public.programs
  WHERE intake_months IS NOT NULL
) sub
GROUP BY 1
ORDER BY n DESC, 1;
