SELECT
    m.partition_name,
    m.inserts,
    m.updates,
    m.deletes,
    TO_CHAR(m.timestamp, 'YYYY-MM-DD HH24:MI:SS') AS flushed_at
FROM all_tab_modifications m
WHERE m.table_owner = :owner
  AND m.table_name = :table_name
  AND m.subpartition_name IS NULL
