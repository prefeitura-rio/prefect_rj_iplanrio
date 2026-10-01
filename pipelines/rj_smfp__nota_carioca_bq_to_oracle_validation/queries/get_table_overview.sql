SELECT
    TO_CHAR(o.created, 'YYYY-MM-DD HH24:MI:SS') AS created,
    TO_CHAR(o.last_ddl_time, 'YYYY-MM-DD HH24:MI:SS') AS last_ddl_time,
    c.comments,
    t.num_rows AS stats_num_rows,
    TO_CHAR(t.last_analyzed, 'YYYY-MM-DD HH24:MI:SS') AS last_analyzed,
    COALESCE(t.tablespace_name, p.def_tablespace_name) AS tablespace_name,
    COALESCE(t.logging, p.def_logging) AS logging,
    COALESCE(t.inmemory, p.def_inmemory) AS inmemory
FROM all_tables t
JOIN all_objects o
    ON o.owner = t.owner
    AND o.object_name = t.table_name
    AND o.object_type = 'TABLE'
LEFT JOIN all_tab_comments c
    ON c.owner = t.owner
    AND c.table_name = t.table_name
LEFT JOIN all_part_tables p
    ON p.owner = t.owner
    AND p.table_name = t.table_name
WHERE t.owner = :owner
    AND t.table_name = :table_name
