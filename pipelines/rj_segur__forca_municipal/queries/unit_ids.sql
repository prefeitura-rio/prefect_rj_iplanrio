SELECT DISTINCT UnitId
FROM `{{ table }}`
WHERE data_particao IN (
    SELECT DISTINCT data_particao FROM `{{ table }}`
    ORDER BY data_particao DESC LIMIT {{ partitions }}
)
    AND UnitId IS NOT NULL
ORDER BY UnitId
