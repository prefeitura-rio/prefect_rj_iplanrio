-- Datasets "dev fantasma" usados pela query em queries_dev/ para o fluxo CVL 1746
-- (cvl_pesquisa_1746_com_solucao.sql / cvl_pesquisa_1746_sem_resolucao.sql). Todos no
-- projeto rj-crm-registry-dev, location US (mesma location dos datasets prod espelhados).
-- A tabela de disparos (status_disparo) fica de fora: continua apontando pro dataset
-- real (rj-crm-registry.brutos_salesforce), igual às demais queries_dev/ do repo.

CREATE SCHEMA IF NOT EXISTS `rj-crm-registry-dev.dev__dev_fantasma__adm_central_atendimento_1746`
OPTIONS (location = 'US');

CREATE SCHEMA IF NOT EXISTS `rj-crm-registry-dev.dev__dev_fantasma__brutos_extracoes_google_sheets`
OPTIONS (location = 'US');
