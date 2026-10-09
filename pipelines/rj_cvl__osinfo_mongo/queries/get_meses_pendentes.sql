-- Query para buscar arquivos pendentes (ainda sem PDF no GCS) agrupados por mes_envio.
-- mes_envio e o ultimo mes em que o arquivo foi enviado (uma linha por filename)
-- Retorna: mes_envio (DATE), filename (STRING)

WITH corte AS (
  SELECT MAX(mes_envio) AS mes_corte
  FROM `rj-agent-cgm-triagem-nf.brutos_osinfo_mongo.vw_files_pdfs_mes_envio`
  WHERE DATE(mes_envio) IN UNNEST(ARRAY<DATE>$meses_envio)
)
SELECT
  DATE(d.mes_envio) AS mes_envio,
  d.filename
FROM `rj-agent-cgm-triagem-nf.brutos_osinfo_mongo.vw_files_pdfs_download` d
CROSS JOIN corte c
WHERE d.n_arquivos_gcs = 0
  AND d.mes_envio <= c.mes_corte
ORDER BY d.mes_envio, d.filename
$bq_files_limit_clause