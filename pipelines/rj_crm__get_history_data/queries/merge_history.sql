MERGE `{{ target }}` AS T
USING `{{ source }}` AS S
ON {% for key in keys %}T.{{ key }} IS NOT DISTINCT FROM S.{{ key }}{% if not loop.last %} AND {% endif %}{% endfor %}
WHEN MATCHED THEN
    UPDATE SET {% for column in update_columns %}{{ column }} = S.{{ column }}{% if not loop.last %}, {% endif %}{% endfor %}
WHEN NOT MATCHED THEN
    INSERT ({{ columns | join(', ') }})
    VALUES ({% for column in columns %}S.{{ column }}{% if not loop.last %}, {% endif %}{% endfor %})
