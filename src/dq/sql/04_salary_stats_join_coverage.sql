WITH s AS (
  SELECT MAX(created_at) AS max_ts
  FROM public.fact_salary
),
p AS (
  SELECT MAX(created_at) AS max_ts
  FROM public.fact_player_stats
),
unioned AS (
  SELECT 'fact_salary' AS table_name, max_ts FROM s
  UNION ALL
  SELECT 'fact_player_stats' AS table_name, max_ts FROM p
)
SELECT
  'max_freshness_lag_hours' AS metric_name,
  COALESCE(
    MAX(
      EXTRACT(EPOCH FROM (NOW() - max_ts)) / 3600.0
    ),
    999999
  ) AS metric_value
FROM unioned;