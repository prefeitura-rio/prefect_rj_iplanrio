-- Criação da tabela com dados das pessoas por evento
CREATE or replace TABLE `rj-crm-registry-dev.teste.cartao_primeira_infancia_carioca_status_teste` (
  NUM_CPF_RESPONSAVEL STRING,
  NOME_RESPONSAVEL STRING,
  NUM_TEL_CONTATO_1_FAM STRING,
  NUM_TEL_CONTATO_2_FAM STRING,
  LOCAL_ENTREGA_PREVISTO STRING,
  ENDERECO_ENTREGA_PREVISTO STRING,
  DATA_ENTREGA_PREVISTA DATE,
  HORA_ENTREGA_PREVISTA STRING,
  CRAS STRING,
  ENDERECO_CRAS STRING,
  DATA_RETIRADA_CRAS DATE,
  HORA_RETIRADA_CRAS STRING,
  ENVELOPE STRING,
  NUM_TEL_CONTATO_1_DECLARADO STRING,
  NOME_SOCIAL_RESPONSAVEL STRING,
  DATA_ENTREGA DATE,
  TIPO_ENTREGA STRING,
  LOCAL_ENTREGA STRING,
  RESPONSAVEL_PELA_RETIRADA STRING,
  STATUS STRING,
  FLAG_ENTREGA BOOLEAN
);

-- Inserção de dados falsos para a tabela com dados das pessoas por evento
INSERT INTO `rj-crm-registry-dev.teste.cartao_primeira_infancia_carioca_status_teste` (
  NUM_CPF_RESPONSAVEL,
  NOME_RESPONSAVEL,
  NUM_TEL_CONTATO_1_FAM,
  NUM_TEL_CONTATO_2_FAM,
  LOCAL_ENTREGA_PREVISTO,
  ENDERECO_ENTREGA_PREVISTO,
  DATA_ENTREGA_PREVISTA,
  HORA_ENTREGA_PREVISTA,
  CRAS,
  ENDERECO_CRAS,
  DATA_RETIRADA_CRAS,
  HORA_RETIRADA_CRAS,
  ENVELOPE,
  NUM_TEL_CONTATO_1_DECLARADO,
  NOME_SOCIAL_RESPONSAVEL,
  DATA_ENTREGA,
  TIPO_ENTREGA,
  LOCAL_ENTREGA,
  RESPONSAVEL_PELA_RETIRADA,
  STATUS,
  FLAG_ENTREGA
)
VALUES
  ('111111111111', 'patricia catandi', '5511984677798', null, 'Cras Vila Isabel', 'Rua Torres Homem, 120 - Vila Isabel', date_add(current_date(), interval 400 day), '10:00', 'CRAS Vila Isabel', 'Rua Torres Homem, 120 - Vila Isabel', date_add(current_date(), interval 400 day), '10:15', 'E001', '(21) 99999-1111', 'Maria S.', date_add(current_date(), interval 400 day), 'retirada no CRAS', 'CRAS Vila Isabel', 'MARIA DA SILVA', 'aguardando evento', TRUE),

  ('98765432100', 'JOÃO PEREIRA', '(21) 97777-3333', '(21) 96666-4444', 'Cras Bangu', 'Rua Fonseca, 300 - Bangu', DATE '2024-10-28', '14:00', 'CRAS Bangu', 'Rua Fonseca, 300 - Bangu', NULL, NULL, 'E002', '(21) 97777-3333', NULL, NULL, 'entrega domiciliar', 'Endereço familiar', NULL, 'aguardando evento', FALSE),

  ('65432198700', 'ANA OLIVEIRA', '(21) 95555-5555', NULL, 'Cras Madureira', 'Rua Carvalho, 85 - Madureira', DATE '2024-10-27', '09:30', 'CRAS Madureira', 'Rua Carvalho, 85 - Madureira', DATE '2024-10-27', '09:45', 'E003', '(21) 95555-5555', 'Ana O.', DATE '2024-10-27', 'retirada no CRAS', 'CRAS Madureira', 'ANA OLIVEIRA', 'retirado', TRUE),

  ('85296374100', 'CARLOS SOUZA', '(21) 94444-6666', '(21) 93333-7777', 'Cras Campo Grande', 'Av. Cesário de Melo, 3200 - Campo Grande', DATE '2024-10-30', '13:00', 'CRAS Campo Grande', 'Av. Cesário de Melo, 3200 - Campo Grande', NULL, NULL, 'E004', '(21) 94444-6666', NULL, NULL, 'entrega domiciliar', 'Endereço familiar', NULL, 'não retirado', FALSE),

  ('11223344556', 'PAULA MENDES', '(21) 92222-8888', NULL, 'Cras Tijuca', 'Rua Conde de Bonfim, 900 - Tijuca', DATE '2024-10-29', '11:00', 'CRAS Tijuca', 'Rua Conde de Bonfim, 900 - Tijuca', DATE '2024-10-29', '11:10', 'E005', '(21) 92222-8888', 'Paula M.', DATE '2024-10-29', 'retirada no CRAS', 'CRAS Tijuca', 'PAULA MENDES', 'retirado', TRUE);



select * from `rj-crm-registry-dev.teste.cartao_primeira_infancia_carioca_status_teste`