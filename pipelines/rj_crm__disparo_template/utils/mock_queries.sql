----- SMS Puérperas SISARE Alta Maternidade ------
CREATE OR REPLACE TABLE `rj-crm-registry-dev.brutos_sms.sisare_alta_maternidade_teste` (
  cpf STRING OPTIONS(description="CPF da gestante."),
  nome STRING OPTIONS(description="Nome completo da gestante."),
  telefone_informado STRING OPTIONS(description="Telefone bruto registrado no SISARE."),
  telefone_valido_whatsapp STRING OPTIONS(description="Telefone validado no formato 55DDD9XXXXXXXX."),
  motivo_invalidacao_telefone STRING OPTIONS(description="Motivo da invalidação do telefone."),
  data_alta_internacao DATE OPTIONS(description="Data da alta hospitalar."),
  cnes_maternidade_alta STRING OPTIONS(description="Código CNES da maternidade da alta."),
  nome_maternidade_alta STRING OPTIONS(description="Nome fantasia da maternidade."),
  data_fim_gestacao DATE OPTIONS(description="Data do fim da gestação."),
  id_desfecho_gestacao INT64 OPTIONS(description="Identificador do desfecho (1=RN Vivo, 2=Óbito/Aborto, 3=RN Internado)."),
  desfecho_gestacao STRING OPTIONS(description="Descrição textual do desfecho.")
);

DELETE FROM `rj-crm-registry-dev.brutos_sms.sisare_alta_maternidade_teste` WHERE true;
INSERT INTO `rj-crm-registry-dev.brutos_sms.sisare_alta_maternidade_teste` 
(
    cpf, 
    nome, 
    telefone_informado, 
    telefone_valido_whatsapp, 
    motivo_invalidacao_telefone, 
    data_alta_internacao, 
    cnes_maternidade_alta, 
    nome_maternidade_alta, 
    data_fim_gestacao, 
    id_desfecho_gestacao, 
    desfecho_gestacao
)
VALUES 
-- Registro da Patricia
(
  '1', 
  'PATRICIA TESTE', 
  '21981111111', 
  '5501981111111',
  NULL, 
  current_date("America/Sao_Paulo"),
  '2269389', 
  'MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA', 
  DATE '2026-03-13', 
  1, 
  'RN Nascido Vivo'
),
-- Registro do Patrick
(
  '5', 
  'PATRICK TESTE', 
  '21971111111', 
  '5', 
  NULL, 
  current_date("America/Sao_Paulo"),
  '2291260', 
  'MATERNIDADE ALEXANDER FLEMING', 
  DATE '2026-03-16', 
  1, 
  'RN Nascido Vivo'
);

----- SMS Puérperas Siscegonha agendamento ------

CREATE OR REPLACE TABLE `rj-crm-registry-dev.brutos_sms.siscegonha_agendamento_maternidade_teste` (
  id_agendamento_gestante STRING OPTIONS(description="Identificador único do agendamento da gestante no SisCegonha."),
  nome STRING OPTIONS(description="Nome da gestante."),
  cpf STRING OPTIONS(description="CPF da gestante."),
  cnes_maternidade_agendada STRING OPTIONS(description="CNES da maternidade agendada para a visita."),
  nome_maternidade_agendada STRING OPTIONS(description="Nome fantasia da maternidade agendada."),
  data_hora_criacao_agendamento DATETIME OPTIONS(description="Data e hora em que o agendamento foi criado no sistema."),
  data_hora_agendamento_visita_maternidade DATETIME OPTIONS(description="Data e hora previstas para a visita."),
  telefones_gestante ARRAY<STRUCT<
    telefone_original STRING,
    origem STRING,
    prioridade STRING,
    telefone_valido_whatsapp STRING,
    motivo_invalidacao_telefone STRING
  >>,
  nome_acompanhante STRING OPTIONS(description="Nome do acompanhante informado."),
  telefone_acompanhante STRUCT<
    telefone_original STRING,
    telefone_valido_whatsapp STRING,
    motivo_invalidacao_telefone STRING
  > OPTIONS(description="Telefone do acompanhante informado.")
);

DELETE FROM `rj-crm-registry-dev.brutos_sms.siscegonha_agendamento_maternidade_teste` WHERE true;
INSERT INTO `rj-crm-registry-dev.brutos_sms.siscegonha_agendamento_maternidade_teste` 
(
    id_agendamento_gestante, 
    nome, 
    cpf, 
    cnes_maternidade_agendada, 
    nome_maternidade_agendada, 
    data_hora_criacao_agendamento, 
    data_hora_agendamento_visita_maternidade, 
    telefones_gestante, 
    nome_acompanhante, 
    telefone_acompanhante
)
VALUES 
-- Registro Completo da Patricia
(
  'AG-PAT-001', 
  'PATRICIA TESTE', 
  --CAST(CAST(FLOOR(RAND() * 100) + 1 AS INT64) AS STRING),
  '1',
  '2269389', 
  'MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',
  current_datetime("America/Sao_Paulo"),
  DATETIME '2026-03-20 14:30:00',
  ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5511980000000', 'SISCEGONHA', '1', '5511980000000', NULL),
    STRUCT('5521981900148', 'SMS_ENRIQUECIMENTO', '2', '5521981900148', NULL),
    STRUCT("5511984677798", 'CADASTRO_ANTIGO', '3', "5511984677798", null)
  ],
  'ACOMPANHANTE DA PATRICIA',
  STRUCT('5511900000000', '5511911111111', NULL)
),
-- Registro Completo do Patrick
(
  'AG-PAT-002', 
  'PATRICK TESTE', 
  --CAST(CAST(FLOOR(RAND() * 100) + 1 AS INT64) AS STRING),
  '5',
  '2291260', 
  'MATERNIDADE ALEXANDER FLEMING',
  current_datetime("America/Sao_Paulo"),
  DATETIME '2026-03-22 08:00:00',
  ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521981900148', 'SISCEGONHA', '1', '5521981900148', NULL),
    STRUCT('5521991618434', 'SMS_ENRIQUECIMENTO', '2', '5521991618434', NULL)
  ],
  'ACOMPANHANTE DO PATRICK',
  STRUCT('5522900000000', '5521900000000', NULL)
);


----- SMS Puérperas Siscegonha agendamento ------