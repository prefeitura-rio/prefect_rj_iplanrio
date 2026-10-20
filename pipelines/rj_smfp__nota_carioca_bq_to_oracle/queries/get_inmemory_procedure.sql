SELECT COUNT(*) AS executable
FROM all_procedures
WHERE owner = :owner
    AND object_name = :name
    AND object_type = 'PROCEDURE'
