WITH source AS (
  SELECT *
  FROM `rj-crm-registry.brutos_salesforce.status_disparo`
  WHERE nome_hsm = {{ campaign }}
    AND processado_datahora >= DATETIME_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 90 MINUTE)
),
total AS (
  SELECT COUNT(DISTINCT cpf) AS total_disparos FROM source
),
aggregated AS (
  SELECT
    status_disparo AS status,
    CASE
      WHEN status_disparo = "sent" THEN 2
      WHEN status_disparo = "delivered" THEN 3
      WHEN status_disparo = "read" THEN 4
      WHEN status_disparo = "failed" THEN 5
    END AS status_ranking,
    COUNT(DISTINCT cpf) AS quantidade_cpfs
  FROM source
  GROUP BY status_disparo, status_ranking
)
SELECT status, quantidade_cpfs, ROUND(quantidade_cpfs * 100.0 / total_disparos, 1) AS percentual
FROM aggregated
CROSS JOIN total
ORDER BY status_ranking
