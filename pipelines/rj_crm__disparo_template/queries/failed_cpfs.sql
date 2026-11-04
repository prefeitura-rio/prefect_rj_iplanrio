SELECT DISTINCT cpf
FROM `rj-crm-registry.brutos_salesforce.status_disparo`
WHERE indicador_quarentena = TRUE
  AND processado_datahora >= DATETIME_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL 4 HOUR)
  AND data_particao >= DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 1 DAY)
  AND LOWER(nome_hsm) = LOWER({{ campaign }})
