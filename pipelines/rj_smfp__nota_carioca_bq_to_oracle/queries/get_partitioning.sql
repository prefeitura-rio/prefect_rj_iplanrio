SELECT partitioning_type, subpartitioning_type, interval
FROM all_part_tables
WHERE owner = :owner
    AND table_name = :table_name
