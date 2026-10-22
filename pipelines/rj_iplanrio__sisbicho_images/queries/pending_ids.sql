SELECT CAST(src.{{ identifier }} AS STRING) AS animal_identifier
FROM `{{ source_table }}` AS src
{% if target_table %}
LEFT JOIN `{{ target_table }}` AS tgt ON CAST(src.{{ identifier }} AS STRING) = tgt.id_animal
{% endif %}
WHERE (src.qrcode_dados IS NOT NULL OR src.foto_dados IS NOT NULL)
{% if target_table %}
    AND tgt.id_animal IS NULL
{% endif %}
GROUP BY animal_identifier
ORDER BY animal_identifier
