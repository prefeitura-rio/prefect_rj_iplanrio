SELECT session_id, phase, bifrost_batch_id, state, input_file_id, row_count, error
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY created_at DESC) AS posicao
  FROM `$table`
)
WHERE posicao = 1
  AND state NOT IN ('done', 'failed')
