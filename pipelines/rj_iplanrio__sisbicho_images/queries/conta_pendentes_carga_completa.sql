-- Animais com QRCode ou foto, sem olhar a tabela destino
-- (tasks.py::_get_total_count, primeira carga ou destino vazio).
SELECT COUNT(*) as total
FROM `${source_table}` AS src
WHERE (src.qrcode_dados IS NOT NULL OR src.foto_dados IS NOT NULL)
