SELECT
  'fact_salary' AS table_name,
  MAX(loaded_at) AS max_loaded_at,
  NOW() - MAX(loaded_at) AS lag
FROM fact_salary;

SELECT
  'fact_player_stats' AS table_name,
  MAX(loaded_at) AS max_loaded_at,
  NOW() - MAX(loaded_at) AS lag
FROM fact_player_stats;