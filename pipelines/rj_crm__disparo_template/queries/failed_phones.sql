WITH status_por_disparo AS (
  SELECT
    flatTarget,
    createDate,
    MAX(
      CASE
        WHEN status = "PROCESSING" THEN 1
        WHEN status = "FAILED" AND (faultdescription LIKE "%131026%" OR faultdescription LIKE "%131048%") THEN 2
        WHEN status = "SENT" THEN 3
        WHEN status = "DELIVERED" THEN 4
        WHEN status = "READ" THEN 5
      END
    ) AS id_status_disparo
  FROM `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*`
  WHERE DATE(createDate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
  GROUP BY flatTarget, createDate
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY flatTarget ORDER BY createDate DESC) AS rn
  FROM status_por_disparo
),
wetalkie_failed AS (
  SELECT flatTarget AS telefone
  FROM ranked
  WHERE rn = 1 AND id_status_disparo = 2
),
sf_failed AS (
  SELECT DISTINCT contato_telefone AS telefone
  FROM `rj-crm-registry.brutos_salesforce.status_disparo`
  WHERE indicador_quarentena = TRUE
)
SELECT telefone FROM wetalkie_failed
UNION DISTINCT
SELECT telefone FROM sf_failed
