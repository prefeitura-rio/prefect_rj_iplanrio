-- Lote de animais com mídia, com o CPF do proprietário atual, sem olhar a tabela destino
-- (tasks.py::fetch_batch, primeira carga ou destino vazio).
WITH animal_unico AS (
    SELECT
        ${identifier_field},
        qrcode_dados,
        foto_dados,
        ROW_NUMBER() OVER (PARTITION BY ${identifier_field} ORDER BY datalake_loaded_at DESC) as rn
    FROM `${source_table}`
    WHERE qrcode_dados IS NOT NULL OR foto_dados IS NOT NULL
)
SELECT
    CAST(a.${identifier_field} AS STRING) AS animal_identifier,
    prop.cpf_numero AS cpf,
    a.qrcode_dados,
    a.foto_dados
FROM animal_unico a
LEFT JOIN `${project_dataset}.animal_proprietario` AS ap
    ON a.${identifier_field} = ap.id_animal
    AND ap.fim_datahora IS NULL
LEFT JOIN `${project_dataset}.proprietario` AS prop
    ON ap.id_proprietario = prop.id_proprietario
WHERE a.rn = 1
LIMIT ${batch_size}
OFFSET ${offset}
