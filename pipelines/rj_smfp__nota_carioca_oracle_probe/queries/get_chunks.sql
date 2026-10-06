SELECT
    chunk_id,
    ROWIDTOCHAR(start_rowid) AS start_rowid,
    ROWIDTOCHAR(end_rowid) AS end_rowid
FROM user_parallel_execute_chunks
WHERE task_name = :task_name
ORDER BY chunk_id
