SELECT DISTINCT cpf, contato_telefone AS telefone, status_disparo AS status, nome_hsm AS nome_campanha, data_particao
FROM `rj-crm-registry.brutos_salesforce.status_disparo`
WHERE data_particao >= DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL {{ interval_days }} DAY)
  AND processado_datahora >= DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL {{ interval_days }} DAY)
