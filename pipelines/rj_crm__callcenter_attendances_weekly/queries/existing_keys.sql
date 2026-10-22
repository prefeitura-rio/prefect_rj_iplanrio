SELECT DISTINCT
    CONCAT(
        CAST(id_ura AS STRING), '|',
        CAST(id_reply AS STRING), '|',
        COALESCE(protocol, 'NULL'), '|',
        CAST(DATE(begin_date) AS STRING), '|',
        CAST(DATE(end_date) AS STRING)
    ) AS composite_key
FROM `{{ table }}`
WHERE DATE(end_date) BETWEEN '{{ start_date }}' AND '{{ end_date }}'
