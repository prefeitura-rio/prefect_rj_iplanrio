SELECT COUNT(*)
FROM all_constraints
WHERE owner = :owner
    AND table_name = :table_name
    AND constraint_type IN ('P', 'U', 'R')
