-- Query para buscar arquivos pendentes (sem URI) agrupados por mes_envio
-- Retorna: mes_envio (DATE), filename (STRING)

WITH corte AS (
  SELECT MAX(mes_envio) AS mes_corte
  FROM `rj-agent-cgm-triagem-nf.brutos_osinfo_mongo.vw_files_pdfs_mes_envio`
  where mes_envio IN UNNEST($meses_envio)
)
SELECT
  d.mes_envio,
  d.filename
FROM `rj-agent-cgm-triagem-nf.brutos_osinfo_mongo.vw_files_pdfs_download` d
CROSS JOIN corte c
WHERE d.sem_duplicacao_a_baixar
  AND d.mes_envio <= c.mes_corte
ORDER BY d.mes_envio
$bq_files_limit_clause