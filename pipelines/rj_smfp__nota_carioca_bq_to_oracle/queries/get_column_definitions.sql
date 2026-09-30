SELECT column_name, data_type, data_length, char_used, data_precision, data_scale, nullable
FROM all_tab_columns
WHERE owner = :owner
    AND table_name = :table_name
ORDER BY column_id
