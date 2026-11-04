SELECT COUNT(*) AS total_sucesso
FROM `rj-crm-registry.brutos_salesforce.status_disparo`
WHERE LOWER(nome_hsm) = LOWER({{ campaign }})
  AND data_particao = DATE({{ dispatch_date }})
  AND (
    envio_meta_datahora >= {{ dispatch_date }}
    OR falha_meta_datahora >= {{ dispatch_date }}
    OR falha_plataforma_datahora >= {{ dispatch_date }}
  )
