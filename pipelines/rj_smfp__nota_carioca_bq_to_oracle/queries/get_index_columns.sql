SELECT c.index_owner, c.index_name, c.column_name, c.descend, e.column_expression
FROM all_ind_columns c
LEFT JOIN all_ind_expressions e
    ON e.index_owner = c.index_owner
    AND e.index_name = c.index_name
    AND e.column_position = c.column_position
WHERE c.table_owner = :owner
    AND c.table_name = :table_name
ORDER BY c.index_owner, c.index_name, c.column_position
