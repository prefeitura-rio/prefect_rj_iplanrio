SELECT
    t.tablespace_name,
    p.def_tablespace_name,
    COALESCE(t.inmemory, p.def_inmemory) AS inmemory,
    COALESCE(t.inmemory_priority, p.def_inmemory_priority) AS inmemory_priority,
    COALESCE(t.inmemory_compression, p.def_inmemory_compression) AS inmemory_compression,
    COALESCE(t.inmemory_distribute, p.def_inmemory_distribute) AS inmemory_distribute,
    COALESCE(t.inmemory_duplicate, p.def_inmemory_duplicate) AS inmemory_duplicate
FROM all_tables t
LEFT JOIN all_part_tables p
    ON p.owner = t.owner
    AND p.table_name = t.table_name
WHERE t.owner = :owner
    AND t.table_name = :table_name
