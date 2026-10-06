SELECT column_name
FROM all_part_key_columns
WHERE owner = :owner
    AND name = :table_name
    AND object_type = 'TABLE'
ORDER BY column_position
