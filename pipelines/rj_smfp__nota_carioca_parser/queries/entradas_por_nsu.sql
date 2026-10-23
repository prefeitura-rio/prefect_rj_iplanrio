-- Entradas do índice para uma lista de NSUs (reavaliação da quarentena).
SELECT
  nsu,
  chave_acesso,
  CAST(tipo_documento AS STRING) AS tipo_documento,
  CAST(tipo_evento AS STRING) AS tipo_evento,
  data_hora_geracao,
  gcs_uri,
  linha,
  xml_sha256,
  origem,
  ambiente
FROM `$project.$dataset.$indice`
WHERE data_hora_geracao >= TIMESTAMP(@desde)
  AND ambiente = @ambiente
  AND nsu IN UNNEST(@nsus)
ORDER BY nsu
