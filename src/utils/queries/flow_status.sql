SELECT ativo, data_limite_disparo, nome_campanha
FROM `rj-crm-registry.brutos_salesforce_staging.disparos_ativos`
WHERE nome_campanha = {{ campaign }} AND ambiente = {{ environment }}
LIMIT 1
