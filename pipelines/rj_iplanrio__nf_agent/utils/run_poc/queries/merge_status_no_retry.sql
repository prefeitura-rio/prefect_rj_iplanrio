-- Same as merge_status_full.sql, but without retry_count (fallback when that
-- column doesn't exist yet on the control table).
MERGE `$status_table` T
USING UNNEST([
  $rows_sql
]) S
  ON CAST(T.id_documento AS STRING) = CAST(S.id_documento AS STRING)
WHEN MATCHED THEN
    UPDATE SET status = S.status, error_message = S.error_message,
               updated_at = S.updated_at
WHEN NOT MATCHED THEN
    INSERT (id_documento, status, error_message, updated_at)
    VALUES (S.id_documento, S.status, IF(S.status = 'erro', S.error_message, NULL),
            S.updated_at)
