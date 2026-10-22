WITH brazil_today AS (
    SELECT DATE(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS local_date
),
alert_stats AS (
    SELECT
        COUNT(DISTINCT CASE WHEN alert_hash = '{{ alert_hash }}' THEN discord_message_id END) AS hash_count_today,
        COUNT(DISTINCT discord_message_id) AS total_alerts_today,
        MAX(CAST(sent_at AS TIMESTAMP)) AS last_alert_sent_at
    FROM `{{ table }}` AS alert_log
    CROSS JOIN brazil_today
    WHERE CAST(alert_log.data_particao AS DATE) = brazil_today.local_date
)
SELECT
    hash_count_today,
    total_alerts_today,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_alert_sent_at, HOUR) AS hours_since_last_alert
FROM alert_stats
