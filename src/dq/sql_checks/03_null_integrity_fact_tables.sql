SELECT
  'fact_salary' AS table_name,
  COUNT(*) FILTER (WHERE player_id IS NULL) AS null_player_id,
  COUNT(*) FILTER (WHERE season IS NULL) AS null_season
FROM fact_salary;

SELECT
  'fact_player_stats' AS table_name,
  COUNT(*) FILTER (WHERE player_id IS NULL) AS null_player_id,
  COUNT(*) FILTER (WHERE season IS NULL) AS null_season
FROM fact_player_stats;