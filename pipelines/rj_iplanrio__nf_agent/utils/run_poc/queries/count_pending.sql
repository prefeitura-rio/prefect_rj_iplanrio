-- Total number of documents still pending processing (unprocessed or
-- errored-but-retryable).
SELECT COUNT(*) AS total
FROM `$input_table` v
LEFT JOIN `$status_table` c
  ON CAST(v.id_documento AS STRING) = CAST(c.id_documento AS STRING)
WHERE c.id_documento IS NULL
   OR (c.status = 'erro' AND (c.retry_count IS NULL OR c.retry_count < $max_retries))
