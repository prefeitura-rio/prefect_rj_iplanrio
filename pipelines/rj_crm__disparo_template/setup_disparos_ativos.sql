-- Tabela de controle (kill-switch operacional) consultada por check_flow_status()
-- em pipelines/rj_crm__disparo_template/utils/dispatch.py.
-- Espelha o schema de rj-crm-registry.brutos_wetalkie_staging.disparos_ativos,
-- mas no dataset novo do Salesforce.
--
-- Como funciona a checagem (check_flow_status):
--   SELECT ativo, data_limite_disparo, nome_campanha
--   FROM disparos_ativos
--   WHERE nome_campanha = '{campaign_name}' AND ambiente = '{flow_environment}'
--   LIMIT 1
-- - Se não achar linha -> flow termina (campanha tratada como inativa).
-- - ativo precisa ser a STRING '1' (qualquer outro valor conta como inativo).
-- - data_limite_disparo NULL = sem data de expiração.
--
-- Estar "ativo" pra disparar de verdade exige DUAS coisas ao mesmo tempo:
--   1) active: true no scheduler do Prefect (senão o flow nem roda)
--   2) uma linha aqui com ativo='1' pro ambiente certo (senão o flow roda e termina cedo)

CREATE TABLE IF NOT EXISTS `rj-crm-registry.brutos_salesforce_staging.disparos_ativos` (
  id_hsm STRING,
  nome_campanha STRING,
  ambiente STRING,
  ativo STRING,
  data_limite_disparo DATE
);

-- Uma linha por campaign_name (coluna I da planilha) x ambiente.
-- smspuerperadisparo6 cobre tanto o D7 (id_hsm 456, desativado no scheduler)
-- quanto o D22 (id_hsm 489, ativo) - o check é só por nome_campanha, não por id_hsm.
-- PIC (smascartaoprimeirainfanciaprodv27 / ...lembreteprodv4) NAO está incluído aqui
-- de propósito: o scheduler deles está active:false e sem query ainda.

INSERT INTO `rj-crm-registry.brutos_salesforce_staging.disparos_ativos`
  (id_hsm, nome_campanha, ambiente, ativo, data_limite_disparo)
VALUES
  ('291', 'cvl_pesquisa_1746_sem_resolucao_prod_v1', 'staging', '1', NULL),
  ('192', 'cvl_pesquisa_1746_prod_v1',               'staging', '1', NULL),
  ('239', 'pgm_divida_ativa_prod_v2',                'staging', '1', NULL),
  ('227', 'pgmdividaativalembreteprodv1',             'staging', '1', NULL),
  ('231', 'pgmdividaativaconfirmapgtoprodv1',         'staging', '1', NULL),
  ('573', '_sms_puerpera_disp1_gestantev2',           'staging', '1', NULL),
  ('610', 'smspuerperasdisparo25',                    'staging', '1', NULL),
  ('588', 'smspuerperasdisparo4v3',                   'staging', '1', NULL),
  ('489', 'smspuerperadisparo6',                      'staging', '1', NULL),
  ('562', 'smspuerperasdisparo9',                     'staging', '1', NULL),
  ('564', 'smspuerperasdisparo12',                    'staging', '1', NULL),
  ('566', 'smspuerperasdisparo14',                    'staging', '1', NULL),
  ('589', 'smspuerperasdisparo152',                   'staging', '1', NULL);
