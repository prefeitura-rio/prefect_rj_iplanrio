SELECT COUNT(*) AS total_sucesso
FROM `rj-crm-registry.brutos_salesforce.status_disparo`
WHERE LOWER(nome_hsm) = LOWER({{ campaign }})
  AND data_particao = DATE({{ dispatch_date }})
  AND (envio_datahora >= {{ dispatch_date }} OR falha_datahora >= {{ dispatch_date }})
