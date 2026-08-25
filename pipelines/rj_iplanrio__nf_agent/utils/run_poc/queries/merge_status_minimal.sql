-- Same as merge_status_full.sql, but without error_message/retry_count
-- (fallback when neither optional column exists on the control table).
MERGE `$status_table` T
USING UNNEST([
  $rows_sql
]) S
  ON CAST(T.id_documento AS STRING) = CAST(S.id_documento AS STRING)
WHEN MATCHED THEN
    UPDATE SET status = S.status, updated_at = S.updated_at
WHEN NOT MATCHED THEN
    INSERT (id_documento, status, updated_at)
    VALUES (S.id_documento, S.status, S.updated_at)
