WITH layout_unique_columns AS (
    SELECT
        column AS coluna_layout,
        MAX(descricao) AS descricao
    FROM `{{ project }}.{{ dataset }}_staging.{{ layout }}`
    GROUP BY 1
),

sheets_unique_columns AS (
    SELECT column AS coluna_sheets
    FROM `{{ project }}.{{ dataset }}_staging.layout_dicionario_colunas`
)

SELECT
    t2.coluna_sheets,
    t1.coluna_layout,
    t1.descricao
FROM layout_unique_columns AS t1
FULL OUTER JOIN sheets_unique_columns AS t2
    ON t1.coluna_layout = t2.coluna_sheets
WHERE t2.coluna_sheets IS NULL
ORDER BY t1.coluna_layout
