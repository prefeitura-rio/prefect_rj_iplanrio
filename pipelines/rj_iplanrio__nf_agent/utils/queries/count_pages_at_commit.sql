-- Distinct pages already processed at the current pipeline version, for
-- the global page cap (see utils/pipeline.py::resolve_submit_budget).
-- Same version key as pending_files.sql: a page counts as "processed"
-- only if it has a row at $current_commit (both "ok" and
-- "erro_processamento" statuses count — an attempted page is final for
-- this version, same as the pending rule).
SELECT COUNT(DISTINCT CONCAT(nome_arquivo, '#', CAST(pagina AS STRING))) AS total_pages
FROM `$extracao_pagina_table`
WHERE pagina IS NOT NULL
  AND versao_pipeline.commit = '$current_commit'
