SELECT
    i.owner,
    i.index_name,
    i.index_type,
    i.uniqueness,
    p.locality,
    COALESCE(i.tablespace_name, p.def_tablespace_name, (
        SELECT MAX(ip.tablespace_name)
        FROM all_ind_partitions ip
        WHERE ip.index_owner = i.owner
            AND ip.index_name = i.index_name
    )) AS tablespace_name,
    i.degree,
    i.status
FROM all_indexes i
LEFT JOIN all_part_indexes p
    ON p.owner = i.owner
    AND p.index_name = i.index_name
WHERE i.table_owner = :owner
    AND i.table_name = :table_name
ORDER BY i.index_name
