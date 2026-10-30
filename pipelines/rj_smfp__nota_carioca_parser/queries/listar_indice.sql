-- Entradas do índice acima da marca, em ordem de NSU. O filtro de data_hora_geracao é exigido pelo
-- require_partition_filter do índice; @desde só limita o início da varredura (o cluster por nsu poda o resto).
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
  AND nsu > @apos_nsu
ORDER BY nsu
LIMIT @limite
