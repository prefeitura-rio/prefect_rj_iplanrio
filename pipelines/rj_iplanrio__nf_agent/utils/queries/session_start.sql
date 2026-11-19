SELECT session_id, phase, bifrost_batch_id, state, input_file_id, row_count, error, contexto
FROM `$table`
WHERE session_id = @session_id
  AND phase = 'classification'
ORDER BY created_at
LIMIT 1
