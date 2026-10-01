SELECT t.tablespace_name, p.def_tablespace_name
FROM all_tables t
LEFT JOIN all_part_tables p
    ON p.owner = t.owner
    AND p.table_name = t.table_name
WHERE t.owner = :owner
    AND t.table_name = :table_name
