-- Contagem real de linhas (tasks.py::_try_query_row_count), para tabela externa
-- em que num_rows do metadado vem 0.
SELECT COUNT(*) AS total FROM `${table}`
