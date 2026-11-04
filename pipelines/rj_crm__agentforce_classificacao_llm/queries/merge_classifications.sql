MERGE `{{ destination }}` AS t
USING `{{ tmp }}` AS s
ON t.{{ primary_key }} = s.{{ primary_key }}
WHEN MATCHED THEN
    UPDATE SET {{ set_clause }}
WHEN NOT MATCHED THEN
    INSERT ({{ insert_cols }})
    VALUES ({{ insert_vals }})
