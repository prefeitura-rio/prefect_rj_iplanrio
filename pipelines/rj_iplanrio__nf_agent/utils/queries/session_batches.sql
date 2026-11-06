SELECT phase, bifrost_batch_id
FROM `$table`
WHERE session_id = @session_id
  AND bifrost_batch_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY phase ORDER BY created_at DESC) = 1
