-- MERGE da staging pra tabela final, por chave + partição.
-- A staging é deduplicada antes (fica a linha mais recente de cada
-- chave+partição): a fonte às vezes repete id (TelemetryTraceSpan: 2 ids
-- repetidos em 7 dias, checado 2026-09-29) e, sem isso, o MERGE falha com
-- "must match at most one source row" — como a staging só é limpa depois de
-- um MERGE bem-sucedido, a duplicata ficava lá e travava todo tick seguinte
-- (telemetry_trace_span parado de 2026-09-09 a 2026-09-29, 368k linhas
-- acumuladas na staging).
-- Placeholders preenchidos em tasks/bigquery.py:merge_staging_to_target.
MERGE `{target}` AS t
USING (
    SELECT *
    FROM `{staging}`
    WHERE TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {primary_key}, {partition_field}
        ORDER BY _loaded_at DESC
    ) = 1
) AS s
ON t.{primary_key} = s.{primary_key}
   AND t.{partition_field} = s.{partition_field}
WHEN MATCHED THEN
    UPDATE SET {set_clause}
WHEN NOT MATCHED THEN
    INSERT ({insert_cols})
    VALUES ({insert_vals})
