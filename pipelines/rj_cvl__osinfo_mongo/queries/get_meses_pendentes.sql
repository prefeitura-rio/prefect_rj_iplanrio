-- Query para buscar arquivos pendentes (sem URI) agrupados por mes_envio
-- Retorna: mes_envio (DATE), filename (STRING)
SELECT
    mes_envio,
    filename
FROM
    `rj-agent-cgm-triagem-nf.brutos_osinfo_mongo.vw_files_pdfs_download`
WHERE
    mes_envio IN UNNEST($meses_envio)
    AND uri IS NULL
LIMIT 1
