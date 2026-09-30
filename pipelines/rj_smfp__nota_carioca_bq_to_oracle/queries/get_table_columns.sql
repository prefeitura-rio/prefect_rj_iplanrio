SELECT column_name
FROM all_tab_columns
WHERE owner = :owner
    AND table_name = :table_name
ORDER BY column_id
