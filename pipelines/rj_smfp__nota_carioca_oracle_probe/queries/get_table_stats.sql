SELECT
    num_rows,
    blocks,
    avg_row_len,
    TO_CHAR(last_analyzed, 'YYYY-MM-DD HH24:MI:SS') AS last_analyzed,
    partitioned,
    TRIM(degree)                                    AS degree,
    compression
FROM all_tables
WHERE owner = :owner
  AND table_name = :table_name
