SELECT DISTINCT Id
FROM `{{ table }}`
WHERE data_particao = (SELECT MAX(data_particao) FROM `{{ table }}`)
    AND Id IS NOT NULL
ORDER BY Id
