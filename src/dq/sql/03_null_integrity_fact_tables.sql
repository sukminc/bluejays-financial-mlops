SELECT
  'fact_salary' AS table_name,
  (
    SUM(CASE WHEN player_id IS NULL THEN 1 ELSE 0 END)
    + SUM(CASE WHEN season IS NULL THEN 1 ELSE 0 END)
    + SUM(CASE WHEN snapshot_date IS NULL THEN 1 ELSE 0 END)
  )::bigint AS null_count
FROM public.fact_salary

UNION ALL

SELECT
  'fact_player_stats' AS table_name,
  (
    SUM(CASE WHEN player_id IS NULL THEN 1 ELSE 0 END)
    + SUM(CASE WHEN season IS NULL THEN 1 ELSE 0 END)
    + SUM(CASE WHEN snapshot_date IS NULL THEN 1 ELSE 0 END)
  )::bigint AS null_count
FROM public.fact_player_stats;