-- NSUs ausentes entre o menor e o maior do ambiente, sem janela de dias. Os NSUs que o ADN confirmou não ter
-- (adn_nsu_inexistente) contam como presentes, para não serem buscados de novo.
WITH conhecidos AS (
  SELECT nsu
  FROM `${projeto}.${dataset}.documentos`
  WHERE ambiente = @ambiente
    AND data_hora_geracao >= TIMESTAMP '1970-01-01'
  UNION DISTINCT
  SELECT nsu
  FROM `${projeto}.${dataset}.adn_nsu_inexistente`
  WHERE ambiente = @ambiente
),
vizinhos AS (
  SELECT nsu AS anterior, LEAD(nsu) OVER (ORDER BY nsu) AS proximo
  FROM conhecidos
)
SELECT anterior + 1 AS nsu_ini, proximo - 1 AS nsu_fim
FROM vizinhos
WHERE proximo - anterior > 1
ORDER BY nsu_ini
LIMIT @limite
