----- SMS Puérperas SISARE Alta Maternidade ------
CREATE OR REPLACE TABLE `rj-crm-registry-dev.brutos_sms.sisare_alta_maternidade_teste` (
  cpf STRING OPTIONS(description="CPF da gestante."),
  nome STRING OPTIONS(description="Nome completo da gestante."),
  data_alta_internacao DATE OPTIONS(description="Data da alta hospitalar."),
  cnes_maternidade_alta STRING OPTIONS(description="Código CNES da maternidade da alta."),
  nome_maternidade_alta STRING OPTIONS(description="Nome fantasia da maternidade."),
  data_parto DATE OPTIONS(description="Data do fim da gestação."),
  id_desfecho_gestacao INT64 OPTIONS(description="Identificador do desfecho (1=RN Vivo, 2=Óbito/Aborto, 3=RN Internado)."),
  desfecho_gestacao STRING OPTIONS(description="Descrição textual do desfecho."),
  telefones_gestante ARRAY<STRUCT<
    telefone_original STRING,
    origem STRING,
    prioridade STRING,
    telefone_valido_whatsapp STRING,
    motivo_invalidacao_telefone STRING
  >>
);

DELETE FROM `rj-crm-registry-dev.brutos_sms.sisare_alta_maternidade_teste` WHERE true;
INSERT INTO `rj-crm-registry-dev.brutos_sms.sisare_alta_maternidade_teste` 
(
    cpf, 
    nome, 
    data_alta_internacao, 
    cnes_maternidade_alta, 
    nome_maternidade_alta, 
    data_parto, 
    id_desfecho_gestacao, 
    desfecho_gestacao, 
    telefones_gestante
)
VALUES 
-- Registro da Patricia
('11', 'PATRICIA TESTE', current_date("America/Sao_Paulo"),'2269389', 'MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA', DATE '2026-03-13', 1, 'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('22', 'RODOLPHO TESTE', current_date("America/Sao_Paulo"),'2269389', 'MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA', DATE '2026-03-13', 1, 'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521992868287', 'SISCEGONHA', '1', '5521992868287', NULL),
    STRUCT('5521992868287', 'SMS_ENRIQUECIMENTO', '2', '5521992868287', NULL),
    STRUCT("5521980375732", 'CADASTRO_ANTIGO', '3', "5521980375732", null)
  ]),
-- Registro do Patrick
('55', 'XICO TESTE', current_date("America/Sao_Paulo"),'2291260', 'MATERNIDADE ALEXANDER FLEMING', DATE '2026-03-16', 1, 'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521981900148', 'SISCEGONHA', '1', '5521981900148', NULL),
    STRUCT('5521991618434', 'SMS_ENRIQUECIMENTO', '2', '5521991618434', NULL),
    STRUCT('5592984212629', 'SMS_ENRIQUECIMENTO', '3', '5592984212629', NULL)
  ]),
-- D-1
('11','TESTE_D-1',date_sub(current_date("America/Sao_Paulo"), interval 1 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
-- D-2
('11','TESTE_D-2',date_sub(current_date("America/Sao_Paulo"), interval 2 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
-- D-3
('11','TESTE_D-3',date_sub(current_date("America/Sao_Paulo"), interval 3 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
-- D-4 até D-42
('11','TESTE_D-4',date_sub(current_date("America/Sao_Paulo"), interval 4 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-5',date_sub(current_date("America/Sao_Paulo"), interval 5 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-6',date_sub(current_date("America/Sao_Paulo"), interval 6 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-7',date_sub(current_date("America/Sao_Paulo"), interval 7 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-8',date_sub(current_date("America/Sao_Paulo"), interval 8 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-9',date_sub(current_date("America/Sao_Paulo"), interval 9 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-10',date_sub(current_date("America/Sao_Paulo"), interval 10 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-11',date_sub(current_date("America/Sao_Paulo"), interval 11 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-12',date_sub(current_date("America/Sao_Paulo"), interval 12 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-13',date_sub(current_date("America/Sao_Paulo"), interval 13 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-14',date_sub(current_date("America/Sao_Paulo"), interval 14 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-15',date_sub(current_date("America/Sao_Paulo"), interval 15 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-16',date_sub(current_date("America/Sao_Paulo"), interval 16 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-17',date_sub(current_date("America/Sao_Paulo"), interval 17 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-18',date_sub(current_date("America/Sao_Paulo"), interval 18 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-19',date_sub(current_date("America/Sao_Paulo"), interval 19 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-20',date_sub(current_date("America/Sao_Paulo"), interval 20 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-21',date_sub(current_date("America/Sao_Paulo"), interval 21 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-22',date_sub(current_date("America/Sao_Paulo"), interval 22 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-23',date_sub(current_date("America/Sao_Paulo"), interval 23 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-24',date_sub(current_date("America/Sao_Paulo"), interval 24 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-25',date_sub(current_date("America/Sao_Paulo"), interval 25 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-26',date_sub(current_date("America/Sao_Paulo"), interval 26 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-27',date_sub(current_date("America/Sao_Paulo"), interval 27 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-28',date_sub(current_date("America/Sao_Paulo"), interval 28 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-29',date_sub(current_date("America/Sao_Paulo"), interval 29 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-30',date_sub(current_date("America/Sao_Paulo"), interval 30 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-31',date_sub(current_date("America/Sao_Paulo"), interval 31 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-32',date_sub(current_date("America/Sao_Paulo"), interval 32 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-33',date_sub(current_date("America/Sao_Paulo"), interval 33 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-34',date_sub(current_date("America/Sao_Paulo"), interval 34 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-35',date_sub(current_date("America/Sao_Paulo"), interval 35 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-36',date_sub(current_date("America/Sao_Paulo"), interval 36 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-37',date_sub(current_date("America/Sao_Paulo"), interval 37 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-38',date_sub(current_date("America/Sao_Paulo"), interval 38 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-39',date_sub(current_date("America/Sao_Paulo"), interval 39 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-40',date_sub(current_date("America/Sao_Paulo"), interval 40 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-41',date_sub(current_date("America/Sao_Paulo"), interval 41 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]),
('11','TESTE_D-42',date_sub(current_date("America/Sao_Paulo"), interval 42 day),'2269389','MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',DATE '2026-03-13',1,'RN Nascido Vivo', ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ]);

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
  '11',
  '2269389', 
  'MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',
  current_datetime("America/Sao_Paulo"),
  DATETIME '2026-03-20 14:30:00',
  ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521964424604', 'SISCEGONHA', '1', '5521964424604', NULL),
    STRUCT("5521984677798", 'CADASTRO_ANTIGO', '3', "5521984677798", null)
  ],
  'ACOMPANHANTE DA PATRICIA',
  STRUCT('5511900000000', '5511911111111', NULL)
),
(
  'AG-PAT-001', 
  'RODOLPHO TESTE', 
  --CAST(CAST(FLOOR(RAND() * 100) + 1 AS INT64) AS STRING),
  '22',
  '2269389', 
  'MATERNIDADE MARIA AMELIA BUARQUE DE HOLLANDA',
  current_datetime("America/Sao_Paulo"),
  DATETIME '2026-03-20 14:30:00',
  ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521992868287', 'SISCEGONHA', '1', '5521992868287', NULL),
    STRUCT('5521992868287', 'SMS_ENRIQUECIMENTO', '2', '5521992868287', NULL),
    STRUCT("5521980375732", 'CADASTRO_ANTIGO', '3', "5521980375732", null)
  ],
  'ACOMPANHANTE',
  STRUCT('5511900000000', '5511911111111', NULL)
),
-- Registro Completo do Patrick
(
  'AG-PAT-002', 
  'Chico TESTE', 
  --CAST(CAST(FLOOR(RAND() * 100) + 1 AS INT64) AS STRING),
  '55',
  '2291260', 
  'MATERNIDADE ALEXANDER FLEMING',
  current_datetime("America/Sao_Paulo"),
  DATETIME '2026-03-22 08:00:00',
  ARRAY<STRUCT<telefone_original STRING, origem STRING, prioridade STRING, telefone_valido_whatsapp STRING, motivo_invalidacao_telefone STRING>>[
    STRUCT('5521981900148', 'SISCEGONHA', '1', '5521981900148', NULL),
    STRUCT('5521991618434', 'SMS_ENRIQUECIMENTO', '2', '5521991618434', NULL),
    STRUCT('5592984212629', 'SMS_ENRIQUECIMENTO', '3', '5592984212629', NULL)
  ],
  'ACOMPANHANTE DO PATRICK',
  STRUCT('5522900000000', '5521900000000', NULL)
);





----- SMAS Agendamento Cadùnico ------

-- 1. Cria a tabela com a estrutura correta dos dados fornecidos
CREATE OR REPLACE TABLE `rj-crm-registry-dev.teste.agendamento_cadunico_teste` (
  nome_sobrenome STRING,
  unidade_cras STRING,
  data DATE,
  hora STRING, -- Mantido como STRING para preservar o formato HH:MM
  endereco STRING,
  telefone STRING, -- Mantido como STRING para não remover o código de país/DDD
  SubscriberKey STRING
);

-- 2. Insere o registro enviado
INSERT INTO `rj-crm-registry-dev.teste.agendamento_cadunico_teste` (
  nome_sobrenome, unidade_cras, data, hora, endereco, telefone, SubscriberKey
)
VALUES (
  'patricia salesforce',
  'CRAS Centro',
  DATE '2026-05-17',
  '10:00',
  'Rua das Flores 123',
  '5511984677798',
  '00000000001'
);