-- =============================================================
-- Script gerado automaticamente a partir dos CSVs do SFTP
-- Dataset: rj-crm-registry-dev.teste
-- Gerado em: 2026-06-22
-- =============================================================


-- -------------------------------------------------------------
-- Tabela: agendamento_cadunico
-- Arquivo: confirma_agendamento_cadunico_all.csv
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.agendamento_cadunico (
    nome_sobrenome  STRING,
    unidade_cras    STRING,
    data            STRING,
    hora            STRING,
    endereco        STRING,
    telefone        STRING,
    SubscriberKey   STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.agendamento_cadunico
    (nome_sobrenome, unidade_cras, data, hora, endereco, telefone, SubscriberKey, LOCALE)
VALUES
    ('patricia salesforce', 'CRAS Centro', '2026-05-17', '10:00', 'Rua das Flores 123', '5511984677798', '00000000001', 'BR'),
    ('gabryelle', 'CRAS Norte', '2026-05-17', '11:30', 'Avenida Brasil 456', '558398414858', '000000000015', 'BR'),
    ('fernando salesforce', 'CRAS Sul', '2026-05-17', '09:00', 'Rua Amazonas 789', '5511960670282', '00000000003', 'BR'),
    ('rodolpho salesforce', 'CRAS Leste', '2026-05-17', '14:00', 'Avenida Paulista 321', '5521980375732', '00000000004', 'BR'),
    -- ('Francisco teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '5592984212629', '00000000005', 'BR'),
    -- ('Diego teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '5518981524292', '00000000006', 'BR'),
    -- ('Ingrid teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '559885427079', '00000000007', 'BR'),
    -- ('Thiago teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '5521994243602', '00000000008', 'BR'),
    -- ('Bruno teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '553499289119', '00000000009', 'BR'),
    -- ('João Pedro teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '5521985573582', '00000000010', 'BR'),
    -- ('Fred teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '553185482592', '00000000011', 'BR'),
    -- ('Bruno M teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '5521992132305', '00000000012', 'BR'),
    -- ('Rodrigo teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '5521999999881', '00000000013', 'BR'),
    ('Vitória teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '5521982941169', '00000000014', 'BR'),
    ('Maria teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '5521989190512', '00000000002', 'BR'),
    ('Maiko teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '5521998580626', '000000000016', 'BR');

-- -------------------------------------------------------------
-- Tabela: cadunico
-- Arquivo: whatsapp_cadunico_202606021352.csv
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.cadunico (
    nome_sobrenome  STRING,
    unidade_cras    STRING,
    data            STRING,
    hora            STRING,
    endereco        STRING,
    telefone        STRING,
    SubscriberKey   STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.cadunico
    (nome_sobrenome, unidade_cras, data, hora, endereco, telefone, SubscriberKey, LOCALE)
VALUES
    ('patricia salesforce', 'CRAS Centro', '2026-05-17', '10:00', 'Rua das Flores 123', '5511984677798', '00000000001', 'BR'),
    ('gabryelle', 'CRAS Norte', '2026-05-17', '11:30', 'Avenida Brasil 456', '558398414858', '000000000015', 'BR'),
    ('fernando salesforce', 'CRAS Sul', '2026-05-17', '09:00', 'Rua Amazonas 789', '5511960670282', '00000000003', 'BR'),
    ('rodolpho salesforce', 'CRAS Leste', '2026-05-17', '14:00', 'Avenida Paulista 321', '5521980375732', '00000000004', 'BR'),
    ('maiko teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '5521998580626', '000000000016', 'BR'),
    ('maria salesforce', 'CRAS Centro', '2026-05-17', '10:00', 'Rua das Flores 123', '5521989190512', '00000000002', 'BR'),
    ('vitoria teste salesforce', 'CRAS Botafogo', '2026-05-17', '09:00', 'Rua Botafogo 142', '5521982941169', '00000000014', 'BR');


-- -------------------------------------------------------------
-- Tabela: pgm_divida_ativa_agradecimento
-- Arquivo: whatsapp_pgm_divida_ativa_agradecimento_202606021350.csv
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.pgm_divida_ativa_agradecimento (
    nome_sobrenome  STRING,
    telefone        STRING,
    SubscriberKey   STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.pgm_divida_ativa_agradecimento
    (nome_sobrenome, telefone, SubscriberKey, LOCALE)
VALUES
    ('patricia salesforce', '5511984677798', '00000000001', 'BR'),
    ('gabryelle', '558398414858', '000000000015', 'BR'),
    ('fernando salesforce', '5511960670282', '00000000003', 'BR'),
    ('rodolpho salesforce', '5521980375732', '00000000004', 'BR'),
    ('maiko teste salesforce', '5521998580626', '000000000016', 'BR'),
    ('maria salesforce', '5521989190512', '00000000002', 'BR'),
    ('vitoria teste salesforce', '5521982941169', '00000000014', 'BR');


-- -------------------------------------------------------------
-- Tabela: pgm_divida_ativa_cobranca
-- Arquivo: whatsapp_pgm_divida_ativa_cobranca_202606021350.csv
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.pgm_divida_ativa_cobranca (
    nome_sobrenome  STRING,
    telefone        STRING,
    SubscriberKey   STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.pgm_divida_ativa_cobranca
    (nome_sobrenome, telefone, SubscriberKey, LOCALE)
VALUES
    ('patricia salesforce', '5511984677798', '00000000001', 'BR'),
    ('gabryelle', '558398414858', '000000000015', 'BR'),
    ('fernando salesforce', '5511960670282', '00000000003', 'BR'),
    ('rodolpho salesforce', '5521980375732', '00000000004', 'BR'),
    ('maiko teste salesforce', '5521998580626', '000000000016', 'BR'),
    ('maria salesforce', '5521989190512', '00000000002', 'BR'),
    ('vitoria teste salesforce', '5521982941169', '00000000014', 'BR');


-- -------------------------------------------------------------
-- Tabela: pgm_divida_ativa_lembrete
-- Arquivo: whatsapp_pgm_divida_ativa_lembrete_202606021350.csv
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.pgm_divida_ativa_lembrete (
    nome_sobrenome  STRING,
    telefone        STRING,
    SubscriberKey   STRING,
    data_vencimento STRING,
    numero_guia     STRING,
    valor_guia      STRING,
    cod_pix         STRING,
    cod_boleto      STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.pgm_divida_ativa_lembrete
    (nome_sobrenome, telefone, SubscriberKey, data_vencimento, numero_guia, valor_guia, cod_pix, cod_boleto, LOCALE)
VALUES
    ('patricia salesforce', '5511984677798', '00000000001', '12/07/2026', '123456', '123,45', 'hj2hj333342xnxkwj3j4jk4', '12343832292028', 'BR'),
    ('gabryelle', '558398414858', '000000000015', '12/07/2026', '123458', '123,45', 'hj2hj333342xnxkwj3j4jk6', '12343832292030', 'BR'),
    ('fernando salesforce', '5511960670282', '00000000003', '12/07/2026', '123459', '123,45', 'hj2hj333342xnxkwj3j4jk7', '12343832292031', 'BR'),
    ('rodolpho salesforce', '5521980375732', '00000000004', '12/07/2026', '123460', '123,45', 'hj2hj333342xnxkwj3j4jk8', '12343832292032', 'BR'),
    ('maiko teste salesforce', '5521998580626', '000000000016', '12/07/2026', '123461', '123,45', 'hj2hj333342xnxkwj3j4jk9', '12343832292033', 'BR'),
    ('maria salesforce', '5521989190512', '00000000002', '12/07/2026', '123457', '123,45', 'hj2hj333342xnxkwj3j4jk5', '12343832292029', 'BR'),
    ('vitoria teste salesforce', '5521982941169', '00000000014', '12/07/2026', '123462', '123,45', 'hj2hj333342xnxkwj3j4jk0', '12343832292034', 'BR');


-- -------------------------------------------------------------
-- Tabela: sms_puerperas_confirma_agendamento
-- Arquivo: whatsapp_sms_puerperas_confirma_agendamento_202606021350.csv
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.sms_puerperas_confirma_agendamento (
    nome            STRING,
    maternidade     STRING,
    dia             STRING,
    hora            STRING,
    telefone        STRING,
    SubscriberKey   STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.sms_puerperas_confirma_agendamento
    (nome, maternidade, dia, hora, telefone, SubscriberKey, LOCALE)
VALUES
    ('patricia salesforce', 'Maria Amelie', '02/02/2026', '02:00', '5511984677798', '00000000001', 'BR'),
    ('gabryelle', 'Maria Amelie', '02/02/2026', '02:00', '558398414858', '000000000015', 'BR'),
    ('fernando salesforce', 'Maria Amelie', '02/02/2026', '02:00', '5511960670282', '00000000003', 'BR'),
    ('rodolpho salesforce', 'Maria Amelie', '02/02/2026', '02:00', '5521980375732', '00000000004', 'BR'),
    ('maiko teste salesforce', 'Maria Amelie', '02/02/2026', '02:00', '5521998580626', '000000000016', 'BR'),
    ('maria salesforce', 'Maria Amelie', '02/02/2026', '02:00', '5521989190512', '00000000002', 'BR'),
    ('vitoria teste salesforce', 'Maria Amelie', '02/02/2026', '02:00', '5521982941169', '00000000014', 'BR');


-- -------------------------------------------------------------
-- Tabela: sms_puerperas_explica_pesquisa
-- Arquivo: whatsapp_sms_puerperas_explica_pesquisa_202606021350.csv
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.sms_puerperas_explica_pesquisa (
    nome            STRING,
    telefone        STRING,
    SubscriberKey   STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.sms_puerperas_explica_pesquisa
    (nome, telefone, SubscriberKey, LOCALE)
VALUES
    ('patricia salesforce', '5511984677798', '00000000001', 'BR'),
    ('gabryelle', '558398414858', '000000000015', 'BR'),
    ('fernando salesforce', '5511960670282', '00000000003', 'BR'),
    ('rodolpho salesforce', '5521980375732', '00000000004', 'BR'),
    ('maiko teste salesforce', '5521998580626', '000000000016', 'BR'),
    ('maria salesforce', '5521989190512', '00000000002', 'BR'),
    ('vitoria teste salesforce', '5521982941169', '00000000014', 'BR');


-- -------------------------------------------------------------
-- Tabela: sms_puerperas_pesquisa1
-- Arquivo: whatsapp_sms_puerperas_pesquisa1_202606021350.csv
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa1 (
    nome            STRING,
    telefone        STRING,
    SubscriberKey   STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa1
    (nome, telefone, SubscriberKey, LOCALE)
VALUES
    ('patricia salesforce', '5511984677798', '00000000001', 'BR'),
    ('gabryelle', '558398414858', '000000000015', 'BR'),
    ('fernando salesforce', '5511960670282', '00000000003', 'BR'),
    ('rodolpho salesforce', '5521980375732', '00000000004', 'BR'),
    ('maiko teste salesforce', '5521998580626', '000000000016', 'BR'),
    ('maria salesforce', '5521989190512', '00000000002', 'BR'),
    ('vitoria teste salesforce', '5521982941169', '00000000014', 'BR');


-- -------------------------------------------------------------
-- Tabela: sms_puerperas_pesquisa2
-- Arquivo: whatsapp_sms_puerperas_pesquisa2_202606021350.csv
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa2 (
    nome            STRING,
    telefone        STRING,
    SubscriberKey   STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa2
    (nome, telefone, SubscriberKey, LOCALE)
VALUES
    ('patricia salesforce', '5511984677798', '00000000001', 'BR'),
    ('gabryelle', '558398414858', '000000000015', 'BR'),
    ('fernando salesforce', '5511960670282', '00000000003', 'BR'),
    ('rodolpho salesforce', '5521980375732', '00000000004', 'BR'),
    ('maiko teste salesforce', '5521998580626', '000000000016', 'BR'),
    ('maria salesforce', '5521989190512', '00000000002', 'BR'),
    ('vitoria teste salesforce', '5521982941169', '00000000014', 'BR');


-- -------------------------------------------------------------
-- Tabela: sms_puerperas_pesquisa3
-- Arquivo: whatsapp_sms_puerperas_pesquisa3_202606021350.csv
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa3 (
    nome            STRING,
    telefone        STRING,
    SubscriberKey   STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa3
    (nome, telefone, SubscriberKey, LOCALE)
VALUES
    ('patricia salesforce', '5511984677798', '00000000001', 'BR'),
    ('gabryelle', '558398414858', '000000000015', 'BR'),
    ('fernando salesforce', '5511960670282', '00000000003', 'BR'),
    ('rodolpho salesforce', '5521980375732', '00000000004', 'BR'),
    ('maiko teste salesforce', '5521998580626', '000000000016', 'BR'),
    ('maria salesforce', '5521989190512', '00000000002', 'BR'),
    ('vitoria teste salesforce', '5521982941169', '00000000014', 'BR');


-- -------------------------------------------------------------
-- Tabela: sms_puerperas_pesquisa4
-- Arquivo: whatsapp_sms_puerperas_pesquisa4_202606021350.csv
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa4 (
    telefone        STRING,
    SubscriberKey   STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa4
    (telefone, SubscriberKey, LOCALE)
VALUES
    ('5511984677798', '00000000001', 'BR'),
    ('558398414858', '000000000015', 'BR'),
    ('5511960670282', '00000000003', 'BR'),
    ('5521980375732', '00000000004', 'BR'),
    ('5521998580626', '000000000016', 'BR'),
    ('5521989190512', '00000000002', 'BR'),
    ('5521982941169', '00000000014', 'BR');


-- -------------------------------------------------------------
-- Tabela: sms_puerperas_pesquisa5
-- Arquivo: whatsapp_sms_puerperas_pesquisa5_202606021350.csv
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa5 (
    nome            STRING,
    telefone        STRING,
    SubscriberKey   STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa5
    (nome, telefone, SubscriberKey, LOCALE)
VALUES
    ('patricia salesforce', '5511984677798', '00000000001', 'BR'),
    ('gabryelle', '558398414858', '000000000015', 'BR'),
    ('fernando salesforce', '5511960670282', '00000000003', 'BR'),
    ('rodolpho salesforce', '5521980375732', '00000000004', 'BR'),
    ('maiko teste salesforce', '5521998580626', '000000000016', 'BR'),
    ('maria salesforce', '5521989190512', '00000000002', 'BR'),
    ('vitoria teste salesforce', '5521982941169', '00000000014', 'BR');


-- -------------------------------------------------------------
-- Tabela: sms_puerperas_pesquisa6
-- Arquivo: whatsapp_sms_puerperas_pesquisa6_202606021350.csv
-- Nota: cabeçalho original duplicado ("LOCALE  ;LOCALE") tratado como coluna única LOCALE
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa6 (
    nome            STRING,
    telefone        STRING,
    SubscriberKey   STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa6
    (nome, telefone, SubscriberKey, LOCALE)
VALUES
    ('patricia salesforce', '5511984677798', '00000000001', 'BR'),
    ('gabryelle', '558398414858', '000000000015', 'BR'),
    ('fernando salesforce', '5511960670282', '00000000003', 'BR'),
    ('rodolpho salesforce', '5521980375732', '00000000004', 'BR'),
    ('maiko teste salesforce', '5521998580626', '000000000016', 'BR'),
    ('maria salesforce', '5521989190512', '00000000002', 'BR'),
    ('vitoria teste salesforce', '5521982941169', '00000000014', 'BR');


-- -------------------------------------------------------------
-- Tabela: sms_puerperas_pesquisa7
-- Arquivo: whatsapp_sms_puerperas_pesquisa7_202606021350.csv
-- -------------------------------------------------------------
CREATE OR REPLACE TABLE `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa7 (
    nome            STRING,
    telefone        STRING,
    SubscriberKey   STRING,
    LOCALE          STRING
);

INSERT INTO `rj-crm-registry-dev`.teste.sms_puerperas_pesquisa7
    (nome, telefone, SubscriberKey, LOCALE)
VALUES
    ('patricia salesforce', '5511984677798', '00000000001', 'BR'),
    ('gabryelle', '558398414858', '000000000015', 'BR'),
    ('fernando salesforce', '5511960670282', '00000000003', 'BR'),
    ('rodolpho salesforce', '5521980375732', '00000000004', 'BR'),
    ('maiko teste salesforce', '5521998580626', '000000000016', 'BR'),
    ('maria salesforce', '5521989190512', '00000000002', 'BR'),
    ('vitoria teste salesforce', '5521982941169', '00000000014', 'BR');
