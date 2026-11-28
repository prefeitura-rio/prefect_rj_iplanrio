-- Lote de animais com mídia ainda fora da tabela destino, com o CPF do proprietário atual
-- (tasks.py::fetch_batch, carga incremental).
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
LEFT JOIN `${target_table}` AS tgt
    ON CAST(a.${identifier_field} AS STRING) = tgt.id_animal
LEFT JOIN `${project_dataset}.animal_proprietario` AS ap
    ON a.${identifier_field} = ap.id_animal
    AND ap.fim_datahora IS NULL
LEFT JOIN `${project_dataset}.proprietario` AS prop
    ON ap.id_proprietario = prop.id_proprietario
WHERE a.rn = 1
  AND tgt.id_animal IS NULL
LIMIT ${batch_size}
OFFSET ${offset}
