SELECT partition_name, high_value, tablespace_name, interval
FROM all_tab_partitions
WHERE table_owner = :owner
    AND table_name = :table_name
ORDER BY partition_position
