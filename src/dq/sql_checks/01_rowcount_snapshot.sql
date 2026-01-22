SELECT
  'fact_salary' AS table_name,
  season,
  COUNT(*) AS row_count
FROM fact_salary
GROUP BY season
ORDER BY season;

SELECT
  'fact_player_stats' AS table_name,
  season,
  COUNT(*) AS row_count
FROM fact_player_stats
GROUP BY season
ORDER BY season;