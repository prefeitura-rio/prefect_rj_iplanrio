SELECT
    t1.* EXCEPT (column),
    t2.* EXCEPT (descricao)
FROM `{{ project }}.{{ dataset }}_staging.{{ layout }}` AS t1
LEFT JOIN `{{ project }}.{{ dataset }}_staging.layout_dicionario_colunas` AS t2
    ON t1.column = t2.column
WHERE t1.table != "{{ skipped }}"
