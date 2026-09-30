SELECT SUM(bytes)
FROM user_segments
WHERE segment_name = :table_name
