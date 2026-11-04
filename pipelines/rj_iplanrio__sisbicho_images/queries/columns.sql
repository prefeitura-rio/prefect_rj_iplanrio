SELECT column_name
FROM `{{ project }}.{{ dataset }}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{{ table }}'
ORDER BY ordinal_position
