/*
  PAN Validation in SQL (PostgreSQL)
  --------------------------------------------------------------
  Author: Mustaquim Areeb
  Context: Small, clean data-quality pipeline to validate Indian PANs.
  Notes:
    - Keeping this as a single, readable script. No fancy frameworks.
    - I like to structure it as: stage → clean → validate → summarize.
    - The dataset I’m using is synthetic; I still mask examples in outputs.
    - If you’re reviewing this, skim the section headers to get the flow.

  How I run it locally (psql):
    -- create database pan_demo; \c pan_demo
    -- \i path/to/pan_validation.sql
    -- then load data (see STAGE block) and run the final SELECTs at the bottom.

  Requirements:
    - PostgreSQL 13+ (I’m testing on PG 16)

  Roadmap later (not in this file):
    - Light Power BI page (summary + “try a PAN” using a parameter)
    - Optional tests/CI if I grow this further
*/

-- ──────────────────────────────────────────────────────────────
-- 0) SAFETY NET: drop leftovers if I’m re-running during dev
-- ──────────────────────────────────────────────────────────────
DROP VIEW  IF EXISTS vw_pan_summary          CASCADE;
DROP VIEW  IF EXISTS vw_invalid_by_reason    CASCADE;
DROP VIEW  IF EXISTS vw_invalid_examples     CASCADE;
DROP VIEW  IF EXISTS vw_reason_pairs         CASCADE;
DROP VIEW  IF EXISTS vw_pan_validations_json CASCADE;
DROP VIEW  IF EXISTS vw_pan_validations      CASCADE;
DROP VIEW  IF EXISTS vw_pan_clean            CASCADE;
DROP FUNCTION IF EXISTS validate_pan(text);
DROP FUNCTION IF EXISTS fn_check_sequence(text);
DROP FUNCTION IF EXISTS fn_check_adjacent_repetition(text);
DROP TABLE IF EXISTS stg_pan_numbers;


-- ──────────────────────────────────────────────────────────────
-- 1) STAGE
--    Raw drop-in table. I keep types super permissive here on purpose.
-- ──────────────────────────────────────────────────────────────
CREATE TABLE stg_pan_numbers (
  pan_raw text
);

-- Loading note (not executable, just how I do it):
--   \copy stg_pan_numbers from 'data/pan_numbers.csv' csv header
-- If your path has spaces, wrap it in quotes. I also like to sanity check
-- row count right after the load.
SELECT count(*) FROM stg_pan_numbers;


-- ──────────────────────────────────────────────────────────────
-- 2) CLEAN
--    Normalize case and whitespace. Blank strings become NULLs.
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_pan_clean AS
SELECT
  pan_raw,
  NULLIF(UPPER(TRIM(pan_raw)), '') AS pan_clean
FROM stg_pan_numbers;


-- ──────────────────────────────────────────────────────────────
-- 3) HELPERS
--    Two small reusable functions: adjacent repetition and ascending sequence.
--    I keep them generic (work on any text) so I can reuse for alpha/digits.
-- ──────────────────────────────────────────────────────────────

-- 3a) Adjacent repetition: returns TRUE when two neighbors are the same.
CREATE OR REPLACE FUNCTION fn_check_adjacent_repetition(p_str text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
  i int;
BEGIN
  IF p_str IS NULL OR length(p_str) < 2 THEN
    RETURN FALSE;
  END IF;
  FOR i IN 1 .. (length(p_str) - 1) LOOP
    IF substring(p_str, i, 1) = substring(p_str, i+1, 1) THEN
      RETURN TRUE;  -- found a repeat
    END IF;
  END LOOP;
  RETURN FALSE;
END;
$$;

-- 3b) Ascending sequence: TRUE when every next char is +1 ASCII (e.g., ABCDE, 1234)
CREATE OR REPLACE FUNCTION fn_check_sequence(p_str text)
RETURNS boolean
LANGUAGE plpgsql AS $$
DECLARE
  i int;
BEGIN
  IF p_str IS NULL OR length(p_str) < 2 THEN
    RETURN FALSE;  -- single char cannot be a sequence
  END IF;
  FOR i IN 1 .. (length(p_str) - 1) LOOP
    IF ascii(substring(p_str, i+1, 1)) - ascii(substring(p_str, i, 1)) <> 1 THEN
      RETURN FALSE;  -- break in the chain → not strictly ascending
    END IF;
  END LOOP;
  RETURN TRUE;  -- made it to the end; it’s a sequence
END;
$$;


-- ──────────────────────────────────────────────────────────────
-- 4) ROW-LEVEL VALIDATION & DIAGNOSTICS
--    This is the heart of the pipeline. I label each row and capture reasons.
--    Rules (per brief):
--      - 10 chars total; pattern AAAAA1234A (A=uppercase letter, 1=digit)
--      - No adjacent repeats in alpha5 or digit4 segments
--      - No ascending sequences in alpha5 or digit4 segments
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_pan_validations AS
SELECT
  c.pan_raw,
  c.pan_clean,
  CASE
    WHEN c.pan_clean IS NULL THEN 'Missing'
    WHEN (
      c.pan_clean ~ '^[A-Z]{5}[0-9]{4}[A-Z]$' AND
      NOT fn_check_adjacent_repetition(substring(c.pan_clean,1,5)) AND
      NOT fn_check_adjacent_repetition(substring(c.pan_clean,6,4)) AND
      NOT fn_check_sequence(substring(c.pan_clean,1,5)) AND
      NOT fn_check_sequence(substring(c.pan_clean,6,4))
    ) THEN 'Valid PAN'
    ELSE 'Invalid PAN'
  END AS status,
  -- reason codes are additive; I like seeing *why* a record failed
  ARRAY_REMOVE(ARRAY[
    CASE WHEN c.pan_clean IS NULL THEN 'MISSING' END,
    CASE WHEN c.pan_clean IS NOT NULL AND length(c.pan_clean) <> 10 THEN 'LEN_NE_10' END,
    CASE WHEN c.pan_clean IS NOT NULL AND c.pan_clean !~ '^[A-Z0-9]+$' THEN 'NON_ALNUM' END,
    CASE WHEN c.pan_clean IS NOT NULL AND c.pan_clean !~ '^[A-Z]{5}[0-9]{4}[A-Z]$' THEN 'PATTERN_FAIL' END,
    CASE WHEN c.pan_clean IS NOT NULL AND c.pan_clean ~ '^[A-Z]{5}' IS FALSE THEN 'FIRST5_NOT_ALPHA' END,
    CASE WHEN c.pan_clean IS NOT NULL AND c.pan_clean ~ '^.....[0-9]{4}' IS FALSE THEN 'MID4_NOT_DIGITS' END,
    CASE WHEN c.pan_clean IS NOT NULL AND fn_check_adjacent_repetition(substring(c.pan_clean,1,5)) THEN 'ADJ_REPEAT_ALPHA' END,
    CASE WHEN c.pan_clean IS NOT NULL AND fn_check_adjacent_repetition(substring(c.pan_clean,6,4)) THEN 'ADJ_REPEAT_DIGITS' END,
    CASE WHEN c.pan_clean IS NOT NULL AND fn_check_sequence(substring(c.pan_clean,1,5)) THEN 'SEQ_ALPHA_ASC' END,
    CASE WHEN c.pan_clean IS NOT NULL AND fn_check_sequence(substring(c.pan_clean,6,4)) THEN 'SEQ_DIGITS_ASC' END
  ], NULL) AS reason_codes
FROM vw_pan_clean c;


-- Optional: JSON mirror (handy if a downstream app wants JSON straight away)
CREATE OR REPLACE VIEW vw_pan_validations_json AS
SELECT
  pan_raw, pan_clean, status,
  to_jsonb(reason_codes) AS reasons_json
FROM vw_pan_validations;


-- ──────────────────────────────────────────────────────────────
-- 5) SUMMARIES (readable, BI-friendly without charts)
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_pan_summary AS
SELECT
  COUNT(*)                                                     AS total_rows,
  COUNT(*) FILTER (WHERE status = 'Valid PAN')                 AS valid_rows,
  COUNT(*) FILTER (WHERE status = 'Invalid PAN')               AS invalid_rows,
  COUNT(*) FILTER (WHERE status = 'Missing')                   AS missing_rows,
  ROUND(100.0 * COUNT(*) FILTER (WHERE status='Valid PAN')   / NULLIF(COUNT(*),0), 2) AS valid_pct,
  ROUND(100.0 * COUNT(*) FILTER (WHERE status='Invalid PAN') / NULLIF(COUNT(*),0), 2) AS invalid_pct
FROM vw_pan_validations;

-- Invalid by reason. I use a window here to show I’m comfortable with it.
CREATE OR REPLACE VIEW vw_invalid_by_reason AS
SELECT
  reason,
  COUNT(*) AS n,
  ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct
FROM vw_pan_validations, UNNEST(reason_codes) AS reason
WHERE status = 'Invalid PAN'
GROUP BY reason
ORDER BY n DESC;

-- Simple masked examples (last char replaced so we’re not echoing a full ID)
CREATE OR REPLACE VIEW vw_invalid_examples AS
WITH r AS (
  SELECT
    REGEXP_REPLACE(pan_clean, '.$', 'X') AS pan_masked,
    UNNEST(reason_codes) AS reason
  FROM vw_pan_validations
  WHERE status = 'Invalid PAN'
)
SELECT reason, pan_masked
FROM r
LIMIT 25;

-- Bonus: pairs of reasons that co-occur. This can hint at rule interactions.
CREATE OR REPLACE VIEW vw_reason_pairs AS
WITH r AS (
  SELECT pan_clean, reason_codes
  FROM vw_pan_validations
  WHERE status = 'Invalid PAN'
)
SELECT LEAST(a,b) AS reason_a,
       GREATEST(a,b) AS reason_b,
       COUNT(*) AS n
FROM r,
     UNNEST(reason_codes) a,
     UNNEST(reason_codes) b
WHERE a < b
GROUP BY LEAST(a,b), GREATEST(a,b)
ORDER BY n DESC;


-- ──────────────────────────────────────────────────────────────
-- 6) ONE-OFF VALIDATOR (nice for demos and Power BI parameter later)
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION validate_pan(p_pan text)
RETURNS TABLE (
  input_pan text,
  status    text,
  reasons   text  -- JSON text for readability in psql/BI
) LANGUAGE sql AS $$
  WITH x AS (
    SELECT NULLIF(UPPER(TRIM(p_pan)), '') AS pan_clean
  ), f AS (
    SELECT
      pan_clean,
      LENGTH(pan_clean) = 10                                  AS len_ok,
      pan_clean ~ '^[A-Z0-9]+$'                               AS alnum_ok,
      pan_clean ~ '^[A-Z]{5}[0-9]{4}[A-Z]$'                   AS pattern_ok,
      fn_check_adjacent_repetition(SUBSTRING(pan_clean,1,5))  AS adj_alpha,
      fn_check_adjacent_repetition(SUBSTRING(pan_clean,6,4))  AS adj_digit,
      fn_check_sequence(SUBSTRING(pan_clean,1,5))             AS seq_alpha_asc,
      fn_check_sequence(SUBSTRING(pan_clean,6,4))             AS seq_digit_asc
    FROM x
  ), r AS (
    SELECT *,
      ARRAY_REMOVE(ARRAY[
        CASE WHEN pan_clean IS NULL THEN 'MISSING' END,
        CASE WHEN pan_clean IS NOT NULL AND LENGTH(pan_clean) <> 10 THEN 'LEN_NE_10' END,
        CASE WHEN pan_clean IS NOT NULL AND pan_clean !~ '^[A-Z0-9]+$' THEN 'NON_ALNUM' END,
        CASE WHEN pan_clean IS NOT NULL AND NOT pattern_ok THEN 'PATTERN_FAIL' END,
        CASE WHEN pan_clean IS NOT NULL AND adj_alpha THEN 'ADJ_REPEAT_ALPHA' END,
        CASE WHEN pan_clean IS NOT NULL AND adj_digit THEN 'ADJ_REPEAT_DIGITS' END,
        CASE WHEN pan_clean IS NOT NULL AND seq_alpha_asc THEN 'SEQ_ALPHA_ASC' END,
        CASE WHEN pan_clean IS NOT NULL AND seq_digit_asc THEN 'SEQ_DIGITS_ASC' END
      ], NULL) AS reason_codes
    FROM f
  )
  SELECT
    p_pan AS input_pan,
    CASE WHEN pan_clean IS NULL THEN 'Missing'
         WHEN reason_codes = '{}' THEN 'Valid PAN'
         ELSE 'Invalid PAN' END AS status,
    TO_JSONB(reason_codes)::text AS reasons
  FROM r;
$$;


-- ──────────────────────────────────────────────────────────────
-- 7) QUICK CHECKS I RUN AFTER LOADING DATA (copy/paste in psql)
-- ──────────────────────────────────────────────────────────────
SELECT * FROM vw_pan_summary;
SELECT * FROM vw_invalid_by_reason LIMIT 10;
SELECT * FROM vw_invalid_examples;
SELECT * FROM vw_reason_pairs LIMIT 10;
SELECT * FROM validate_pan('ABCDE1234F');


-- ──────────────────────────────────────────────────────────────
-- 8) OPTIONAL EXPORTS (uncomment if you want CSVs for README/BI)
--    Make sure the path is writable by the server. On local dev, I often
--    use \copy from the client instead of server-side COPY.
-- ──────────────────────────────────────────────────────────────
-- -- server-side COPY (adjust path):
-- COPY (
--   SELECT * FROM vw_pan_summary
-- ) TO '/tmp/summary.csv' CSV HEADER;
-- 
-- COPY (
--   SELECT * FROM vw_invalid_by_reason
-- ) TO '/tmp/invalid_by_reason.csv' CSV HEADER;
-- 
-- COPY (
--   SELECT * FROM vw_invalid_examples
-- ) TO '/tmp/invalid_examples.csv' CSV HEADER;

/* End of file */
