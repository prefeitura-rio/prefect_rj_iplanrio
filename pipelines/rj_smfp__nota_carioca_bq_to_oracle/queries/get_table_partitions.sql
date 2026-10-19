SELECT partition_name, high_value, tablespace_name, interval,
    segment_created, initial_extent, next_extent, min_extent, max_extent, pct_increase
FROM all_tab_partitions
WHERE table_owner = :owner
    AND table_name = :table_name
ORDER BY partition_position
