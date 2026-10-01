SELECT
    COUNT(DISTINCT NVL(partition_name, '-')) AS populated_segments,
    NVL(SUM(bytes_not_populated), 0) AS bytes_not_populated,
    COUNT(CASE WHEN populate_status <> 'COMPLETED' THEN 1 END) AS incomplete_segments
FROM gv$$im_segments
WHERE owner = :owner
    AND segment_name = :table_name
