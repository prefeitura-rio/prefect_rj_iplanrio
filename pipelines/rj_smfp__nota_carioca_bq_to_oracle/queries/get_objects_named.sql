SELECT object_type
FROM all_objects
WHERE owner = :owner
    AND object_name = :object_name
