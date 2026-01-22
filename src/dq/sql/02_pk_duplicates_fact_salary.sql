WITH dups AS (
  SELECT
    player_id,
    season,
    snapshot_date,
    COUNT(*) AS c
  FROM public.fact_salary
  GROUP BY player_id, season, snapshot_date
  HAVING COUNT(*) > 1
)
SELECT
  'fact_salary' AS table_name,
  COALESCE(SUM(c - 1), 0)::bigint AS dup_count
FROM dups;