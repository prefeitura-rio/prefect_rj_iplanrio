SELECT COUNT(*) AS total
FROM `rj-crm-registry.brutos_salesforce.jornada`
WHERE hsm.nome_hsm = {{ campaign }}
