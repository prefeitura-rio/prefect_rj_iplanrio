WITH ranked_messages AS (
  SELECT
    targetExternalId,
    status,
    ROW_NUMBER() OVER (PARTITION BY targetExternalId ORDER BY datarelay_timestamp DESC) AS rn
  FROM `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*`
  WHERE templateId = {{ id_hsm }}
    AND sendDate >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
),
latest_status AS (
  SELECT status, targetExternalId FROM ranked_messages WHERE rn = 1
),
total AS (
  SELECT COUNT(DISTINCT targetExternalId) AS total_cpfs FROM latest_status
),
aggregated AS (
  SELECT
    status,
    CASE
      WHEN status = "PROCESSING" THEN 1
      WHEN status = "SENT" THEN 2
      WHEN status = "DELIVERED" THEN 3
      WHEN status = "READ" THEN 4
      WHEN status = "FAILED" THEN 5
      ELSE 6
    END AS status_ranking,
    COUNT(*) AS quantidade_cpfs
  FROM latest_status
  GROUP BY status, status_ranking
)
SELECT status, quantidade_cpfs, ROUND(quantidade_cpfs * 100.0 / total_cpfs, 1) AS percentual
FROM aggregated
CROSS JOIN total
ORDER BY status_ranking
