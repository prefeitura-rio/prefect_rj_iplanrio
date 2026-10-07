SELECT name, value
FROM v$$parameter
WHERE name IN (
    'cpu_count',
    'parallel_max_servers',
    'undo_retention',
    'undo_management',
    'undo_tablespace',
    'db_block_size',
    'db_file_multiblock_read_count'
)
ORDER BY name
