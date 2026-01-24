SELECT COUNT(*)
FROM stg_spotrac_bluejays_salary_raw
WHERE player_name IS NULL OR player_name = '';