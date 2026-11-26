SELECT filename AS name, pdf.uri AS uri
FROM `$table`
WHERE n_arquivos_gcs > 0
  AND pdf.uri IS NOT NULL
  AND DATE(mes_envio) = @mes_envio
ORDER BY filename
