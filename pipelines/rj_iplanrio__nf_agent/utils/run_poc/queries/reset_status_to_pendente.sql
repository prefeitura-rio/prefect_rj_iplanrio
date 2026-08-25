-- Force-reprocess: resets every 'processado'/'erro' row in the control table
-- back to 'pendente' so the next batch picks them all up again.
UPDATE `$status_table`
SET status = 'pendente', updated_at = CURRENT_TIMESTAMP()
WHERE status IN ('processado', 'erro')
