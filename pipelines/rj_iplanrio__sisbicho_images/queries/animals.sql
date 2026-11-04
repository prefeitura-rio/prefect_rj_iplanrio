WITH latest AS (
    SELECT
        CAST({{ identifier }} AS STRING) AS animal_identifier,
        {{ identifier }} AS raw_identifier,
        qrcode_dados,
        foto_dados,
        ROW_NUMBER() OVER (PARTITION BY {{ identifier }} ORDER BY datalake_loaded_at DESC) AS position
    FROM `{{ source_table }}`
    WHERE (qrcode_dados IS NOT NULL OR foto_dados IS NOT NULL)
        AND CAST({{ identifier }} AS STRING) IN ({% for id in ids %}'{{ id }}'{% if not loop.last %}, {% endif %}{% endfor %})
)
SELECT
    latest.animal_identifier,
    owner.cpf_numero AS cpf,
    latest.qrcode_dados,
    latest.foto_dados
FROM latest
LEFT JOIN `{{ dataset_path }}.animal_proprietario` AS link
    ON latest.raw_identifier = link.id_animal AND link.fim_datahora IS NULL
LEFT JOIN `{{ dataset_path }}.proprietario` AS owner ON link.id_proprietario = owner.id_proprietario
WHERE latest.position = 1
ORDER BY latest.animal_identifier
