SELECT SUM(bytes)
FROM dba_segments
WHERE owner = :owner
    AND segment_name = :table_name
