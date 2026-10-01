SELECT
    CASE
        WHEN t.partitioned = 'YES' THEN (
            SELECT COUNT(*)
            FROM all_tab_partitions p
            WHERE p.table_owner = t.owner
                AND p.table_name = t.table_name
                AND p.num_rows > 0
        )
        WHEN t.num_rows > 0 THEN 1
        ELSE 0
    END AS expected_segments
FROM all_tables t
WHERE t.owner = :owner
    AND t.table_name = :table_name
