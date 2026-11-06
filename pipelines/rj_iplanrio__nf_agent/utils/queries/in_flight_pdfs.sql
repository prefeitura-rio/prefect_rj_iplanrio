WITH ativas AS (
  SELECT session_id
  FROM (
    SELECT
      session_id,
      state,
      ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY created_at DESC) AS posicao
    FROM `$table`
  )
  WHERE posicao = 1
    AND state NOT IN ('done', 'failed')
)
SELECT DISTINCT JSON_VALUE(pdf, '$$.name') AS nome_arquivo
FROM `$table` AS eventos
JOIN ativas USING (session_id),
  UNNEST(JSON_QUERY_ARRAY(eventos.contexto, '$$.pdfs')) AS pdf
WHERE eventos.phase = 'classification'
  AND eventos.contexto IS NOT NULL
