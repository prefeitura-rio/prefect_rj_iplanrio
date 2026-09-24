-- Files (nome_arquivo) among the candidates that are already fully done at
-- the current pipeline version: every page known for the file from any past
-- run/version (MAX(pagina)) also has a row at $current_commit. Both
-- pipeline_status values ("ok" and "erro_processamento") count as "already
-- attempted this version" — there's no automatic cross-run retry.
--
-- `extracao_pagina` is a BigQuery external table over NDJSON in GCS with
-- an autodetected schema. `versao_pipeline` is written as a real nested
-- JSON object (see GCSResultsWriter.write_ndjson), not a JSON-encoded
-- string — so BigQuery infers it as a STRUCT/RECORD column, never
-- STRING/JSON. Access its `commit` field with dot notation accordingly
-- (a `JSON_VALUE(versao_pipeline, '$$.commit')` call here would fail with
-- "Unable to coerce type STRUCT<...> to expected type STRING").
--
-- The caller subtracts this result from the candidate set to get the
-- pending (still-to-process) files.
WITH known AS (
  SELECT
    nome_arquivo,
    MAX(pagina) AS max_pagina_conhecida
  FROM `$extracao_pagina_table`
  WHERE pagina IS NOT NULL
    AND nome_arquivo IN ($candidate_filenames)
  GROUP BY nome_arquivo
),
versao_atual AS (
  SELECT
    nome_arquivo,
    COUNT(DISTINCT pagina) AS paginas_versao_atual
  FROM `$extracao_pagina_table`
  WHERE pagina IS NOT NULL
    AND nome_arquivo IN ($candidate_filenames)
    AND versao_pipeline.commit = '$current_commit'
  GROUP BY nome_arquivo
)
SELECT
  k.nome_arquivo
FROM known k
JOIN versao_atual v USING (nome_arquivo)
WHERE v.paginas_versao_atual >= k.max_pagina_conhecida
