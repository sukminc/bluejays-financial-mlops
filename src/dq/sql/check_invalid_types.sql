-- Failure Condition: Salary is not a valid currency format
-- Valid: $10,000 or 10000. Invalid: 'Unknown', 'InvalidData'
-- [FIX] Added CAST(... AS TEXT) to prevent "operator does not exist: bigint !~ unknown" errors
SELECT COUNT(*)
FROM stg_spotrac_bluejays_salary_raw
WHERE CAST(luxury_tax_usd AS TEXT) !~ '^\$?[0-9,]+(\.[0-9]+)?$';