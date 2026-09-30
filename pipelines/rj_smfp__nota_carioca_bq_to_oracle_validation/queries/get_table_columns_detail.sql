SELECT column_id, column_name, data_type, data_length, data_precision, data_scale, char_used, nullable
FROM all_tab_columns
WHERE owner = :owner
    AND table_name = :table_name
ORDER BY column_id
