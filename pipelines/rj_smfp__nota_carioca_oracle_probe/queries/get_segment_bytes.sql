SELECT SUM(bytes) AS bytes, COUNT(*) AS segments
FROM dba_segments
WHERE owner = :owner
  AND segment_name = :table_name
  AND segment_type LIKE 'TABLE%'
