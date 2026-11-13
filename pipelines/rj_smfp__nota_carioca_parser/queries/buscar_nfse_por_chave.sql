-- A NFS-e de uma chave de acesso (a de menor NSU), para o evento herdar a prioridade da quarentena.
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
  AND chave_acesso = @chave_acesso
  AND UPPER(CAST(tipo_documento AS STRING)) IN ('NFSE', '1')
ORDER BY nsu
LIMIT 1
