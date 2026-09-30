SELECT COUNT(*)
FROM all_indexes
WHERE table_owner = :owner
    AND table_name = :table_name
