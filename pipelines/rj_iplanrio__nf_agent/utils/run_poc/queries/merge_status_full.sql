-- Upsert processing status into the control table, tracking retry_count.
-- Requires the optional columns error_message/retry_count to already exist:
--   ALTER TABLE `<status_table>` ADD COLUMN IF NOT EXISTS error_message STRING;
--   ALTER TABLE `<status_table>` ADD COLUMN IF NOT EXISTS retry_count INT64;
-- $rows_sql is a list of STRUCT(...) literals built in Python from the batch
-- of status rows being upserted.
MERGE `$status_table` T
USING UNNEST([
  $rows_sql
]) S
  ON CAST(T.id_documento AS STRING) = CAST(S.id_documento AS STRING)
WHEN MATCHED AND S.status = 'erro' THEN
    UPDATE SET status = S.status, error_message = S.error_message,
               retry_count = COALESCE(T.retry_count, 0) + 1,
               updated_at = S.updated_at
WHEN MATCHED THEN
    UPDATE SET status = S.status, error_message = S.error_message,
               updated_at = S.updated_at
WHEN NOT MATCHED THEN
    INSERT (id_documento, status, error_message, retry_count, updated_at)
    VALUES (S.id_documento, S.status, IF(S.status = 'erro', S.error_message, NULL),
            IF(S.status = 'erro', 1, 0), S.updated_at)
