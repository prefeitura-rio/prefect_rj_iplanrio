SELECT t.table_name, c.comments
FROM all_tables t
LEFT JOIN all_tab_comments c
    ON c.owner = t.owner
    AND c.table_name = t.table_name
WHERE t.owner = :owner
    AND t.table_name = :table_name
