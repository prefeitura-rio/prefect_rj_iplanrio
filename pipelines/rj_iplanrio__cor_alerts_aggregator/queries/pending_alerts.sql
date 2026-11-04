{% set neighborhood %}
COALESCE(
    NULLIF(alerts.bairro_normalizado, ''),
    CASE
        WHEN REGEXP_CONTAINS(LOWER(alerts.address), r'\bacari\b') THEN 'acari'
        WHEN REGEXP_CONTAINS(LOWER(alerts.address), r'\b(jardim\s+am[eé]rica|jd\s+am[eé]rica)\b') THEN 'jardim america'
        WHEN REGEXP_CONTAINS(LOWER(alerts.address), r'\bguaratiba\b') THEN 'guaratiba'
        ELSE ''
    END
)
{% endset %}
SELECT
    alerts.alert_id,
    COALESCE(alerts.user_id, '') AS user_id,
    alerts.alert_type,
    LOWER(COALESCE(alerts.severity, '')) AS severity,
    COALESCE(alerts.description, '') AS description,
    COALESCE(alerts.address, '') AS address,
    alerts.latitude,
    alerts.longitude,
    {{ neighborhood }} AS neighborhood,
    CAST(alerts.created_at AS DATETIME) AS created_at
FROM `{{ history_table }}` AS alerts
{% if sent_table %}
LEFT JOIN `{{ sent_table }}` AS sent ON alerts.alert_id = sent.alert_id
{% endif %}
WHERE {% if sent_table %}sent.alert_id IS NULL AND {% endif %}alerts.latitude IS NOT NULL
    AND alerts.longitude IS NOT NULL
    AND CAST(alerts.created_at AS DATETIME) >= DATETIME_SUB(
        CURRENT_DATETIME('America/Sao_Paulo'), INTERVAL {{ window_minutes + 3 }} MINUTE
    )
    AND {{ neighborhood }} IN ({% for name in neighborhoods %}'{{ name }}'{% if not loop.last %}, {% endif %}{% endfor %})
ORDER BY created_at ASC
