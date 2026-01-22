SELECT
  player_id,
  season,
  snapshot_date,
  COUNT(*) AS dup_cnt
FROM fact_salary
GROUP BY player_id, season, snapshot_date
HAVING COUNT(*) > 1
ORDER BY dup_cnt DESC;