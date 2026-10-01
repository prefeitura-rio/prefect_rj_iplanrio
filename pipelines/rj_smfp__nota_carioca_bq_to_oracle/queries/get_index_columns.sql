SELECT index_owner, index_name, column_name
FROM all_ind_columns
WHERE table_owner = :owner
    AND table_name = :table_name
ORDER BY index_owner, index_name, column_position
