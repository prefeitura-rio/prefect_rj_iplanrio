SELECT ativo, data_limite_disparo, nome_hsm, nome_campanha
FROM `rj-crm-registry.brutos_salesforce_staging.disparos_ativos`
WHERE nome_hsm = {{ campaign }} AND ambiente = {{ environment }}
LIMIT 1
