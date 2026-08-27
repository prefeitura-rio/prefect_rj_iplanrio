-- Document and distinct-PDF counts grouped by status
-- ('processado', 'erro', or 'pendente' when no status row exists yet).
SELECT
    COALESCE(c.status, 'pendente') AS status,
    COUNT(*) AS total_docs,
    COUNT(DISTINCT COALESCE(v.descricao, CAST(v.id_documento AS STRING))) AS total_pdfs
FROM `$input_table` v
LEFT JOIN `$status_table` c
  ON CAST(v.id_documento AS STRING) = CAST(c.id_documento AS STRING)
GROUP BY 1
