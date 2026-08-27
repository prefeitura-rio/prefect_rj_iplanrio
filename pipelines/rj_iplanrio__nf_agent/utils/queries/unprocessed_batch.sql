-- All rows for up to $batch_size distinct PDFs that are unprocessed
-- (never attempted) or errored-but-retryable (retry_count < $max_retries).
-- Grouping by descricao (view already strips .pdf suffix); COALESCE to
-- id_documento guards against NULL descricao so the INNER JOIN never drops
-- rows due to NULL = NULL being FALSE in SQL.
WITH unprocessed AS (
    SELECT v.*
    FROM `$input_table` v
    LEFT JOIN `$status_table` c
      ON CAST(v.id_documento AS STRING) = CAST(c.id_documento AS STRING)
    WHERE c.id_documento IS NULL
       OR (c.status = 'erro' AND (c.retry_count IS NULL OR c.retry_count < $max_retries))
),
batch_pdf_keys AS (
    SELECT DISTINCT
        COALESCE(descricao, CAST(id_documento AS STRING)) AS pdf_key
    FROM unprocessed
    LIMIT $batch_size
)
SELECT u.*
FROM unprocessed u
INNER JOIN batch_pdf_keys bk
  ON COALESCE(u.descricao, CAST(u.id_documento AS STRING)) = bk.pdf_key
