SELECT tablespace_name, status, retention, block_size
FROM dba_tablespaces
WHERE contents = 'UNDO'
ORDER BY tablespace_name
