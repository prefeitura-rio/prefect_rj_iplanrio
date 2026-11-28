-- MERGE completo da carga (tasks/load.py::carrega_classificacoes): upsert por id_sessao que
-- sobrescreve todas as colunas com o valor da tmp. As listas de colunas são montadas em
-- Python a partir do SCHEMA (set_clause, insert_cols, insert_vals) — STYLEGUIDE §7.4.
MERGE `${destino_full}` AS t
USING `${tmp_full}` AS s
ON t.${primary_key} = s.${primary_key}
WHEN MATCHED THEN
    UPDATE SET ${set_clause}
WHEN NOT MATCHED THEN
    INSERT (${insert_cols})
    VALUES (${insert_vals})
