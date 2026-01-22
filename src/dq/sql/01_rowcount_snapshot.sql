SELECT 'dim_players' AS table_name, COUNT(*)::bigint AS row_count
FROM dim_players
UNION ALL
SELECT 'fact_salary' AS table_name, COUNT(*)::bigint AS row_count
FROM fact_salary
UNION ALL
SELECT 'fact_player_stats' AS table_name, COUNT(*)::bigint AS row_count
FROM fact_player_stats;