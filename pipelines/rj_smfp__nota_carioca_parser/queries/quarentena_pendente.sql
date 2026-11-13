-- NSUs cuja última avaliação foi uma recusa de outra versão do parser (prioridade nula = devolvido), P1 primeiro.
SELECT nsu
FROM (
  SELECT
    nsu,
    prioridade,
    versao_parser,
    ROW_NUMBER() OVER (PARTITION BY nsu ORDER BY avaliado_em DESC) AS ordem
  FROM `$project.$dataset.$quarentena`
)
WHERE ordem = 1
  AND prioridade IS NOT NULL
  AND versao_parser != @versao_parser
ORDER BY prioridade, nsu
LIMIT @limite
