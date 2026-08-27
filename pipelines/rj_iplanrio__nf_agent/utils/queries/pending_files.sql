-- Files (nome_arquivo) among the candidates that are already fully done at
-- the current pipeline version: every page known for the file from any past
-- run/version (MAX(pagina)) also has a row at $current_commit. Both
-- pipeline_status values ("ok" and "erro_processamento") count as "already
-- attempted this version" — there's no automatic cross-run retry.
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
    AND JSON_VALUE(versao_pipeline, '$$.commit') = '$current_commit'
  GROUP BY nome_arquivo
)
SELECT
  k.nome_arquivo
FROM known k
JOIN versao_atual v USING (nome_arquivo)
WHERE v.paginas_versao_atual >= k.max_pagina_conhecida
