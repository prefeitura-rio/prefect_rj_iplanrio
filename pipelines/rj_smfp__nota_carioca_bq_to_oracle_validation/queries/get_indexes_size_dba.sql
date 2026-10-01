SELECT SUM(s.bytes)
FROM dba_segments s
JOIN all_indexes i
    ON i.owner = s.owner
    AND i.index_name = s.segment_name
WHERE i.table_owner = :owner
    AND i.table_name = :table_name
