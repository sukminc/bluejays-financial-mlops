SELECT COUNT(*)
FROM stg_spotrac_bluejays_salary_raw
WHERE luxury_tax_usd !~ '^\$?[0-9,]+(\.[0-9]+)?$';