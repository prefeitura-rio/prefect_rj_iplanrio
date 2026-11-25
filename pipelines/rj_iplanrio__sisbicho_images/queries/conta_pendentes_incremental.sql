-- Animais com QRCode ou foto que ainda não estão na tabela destino
-- (tasks.py::_get_total_count, carga incremental).
SELECT COUNT(*) as total
FROM `${source_table}` AS src
LEFT JOIN `${target_table}` AS tgt
    ON CAST(src.${identifier_field} AS STRING) = tgt.id_animal
WHERE (src.qrcode_dados IS NOT NULL OR src.foto_dados IS NOT NULL)
  AND tgt.id_animal IS NULL
