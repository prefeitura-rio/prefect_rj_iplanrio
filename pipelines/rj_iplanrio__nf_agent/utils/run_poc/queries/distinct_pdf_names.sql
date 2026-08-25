-- Unique PDF filenames present in the despesas_recorte table.
SELECT DISTINCT descricao_limpa AS pdf_name
FROM `$project.$dataset.osinfo_despesas_recorte`
ORDER BY pdf_name
