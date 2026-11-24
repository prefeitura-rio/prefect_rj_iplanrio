WITH marks AS (
    SELECT DISTINCT
        last_seq,
        seq IS NOT NULL AS changed,
        TRY_CAST(split_part(last_seq, '-', 1) AS HUGEINT) AS number
    FROM rows
)
SELECT
    max_by(last_seq, number) FILTER (WHERE changed AND number IS NOT NULL),
    min_by(last_seq, number) FILTER (WHERE NOT changed AND number IS NOT NULL)
FROM marks
