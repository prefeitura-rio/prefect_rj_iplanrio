SELECT partition_name
FROM all_tab_partitions
WHERE table_owner = :owner
    AND table_name = :table_name
    AND num_rows > 0
    AND inmemory = 'ENABLED'
ORDER BY partition_position
