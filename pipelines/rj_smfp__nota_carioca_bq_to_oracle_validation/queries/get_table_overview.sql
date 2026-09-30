SELECT
    TO_CHAR(o.created, 'YYYY-MM-DD HH24:MI:SS') AS created,
    TO_CHAR(o.last_ddl_time, 'YYYY-MM-DD HH24:MI:SS') AS last_ddl_time,
    c.comments,
    t.num_rows AS stats_num_rows,
    TO_CHAR(t.last_analyzed, 'YYYY-MM-DD HH24:MI:SS') AS last_analyzed,
    t.tablespace_name,
    t.logging
FROM all_tables t
JOIN all_objects o
    ON o.owner = t.owner
    AND o.object_name = t.table_name
    AND o.object_type = 'TABLE'
LEFT JOIN all_tab_comments c
    ON c.owner = t.owner
    AND c.table_name = t.table_name
WHERE t.owner = :owner
    AND t.table_name = :table_name
