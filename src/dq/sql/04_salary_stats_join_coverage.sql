WITH joined AS (
  SELECT
    f.player_id,
    f.season,
    CASE WHEN s.player_id IS NULL THEN 0 ELSE 1 END AS has_stats
  FROM fact_salary f
  LEFT JOIN fact_player_stats s
    ON s.player_id = f.player_id
   AND s.season = f.season
)
SELECT
  season,
  COUNT(*) AS salary_rows,
  SUM(has_stats) AS matched_rows,
  ROUND((SUM(has_stats)::numeric / NULLIF(COUNT(*), 0)) * 100, 2) AS join_pct
FROM joined
GROUP BY season
ORDER BY season;